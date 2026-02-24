# 📋 Integração Duplo CPC — WhatsApp BancoBV (Paschoalotto)

> Documento completo de implementação passo a passo.
> Última atualização: **23/02/2026**

---

## 📖 Índice

1. [Visão Geral](#1-visão-geral)
2. [Diagrama de Fluxo](#2-diagrama-de-fluxo)
3. [API do Parceiro (Paschoalotto)](#3-api-do-parceiro-paschoalotto)
4. [Variáveis de Ambiente](#4-variáveis-de-ambiente)
5. [Backend — Módulo CPC](#5-backend--módulo-cpc)
6. [Backend — Bloqueio Outbound (WebSocket Gateway)](#6-backend--bloqueio-outbound-websocket-gateway)
7. [Backend — Registro Inbound (Webhooks)](#7-backend--registro-inbound-webhooks)
8. [Frontend — Modal de Nova Conversa 1x1](#8-frontend--modal-de-nova-conversa-1x1)
9. [Resumo dos Arquivos Modificados](#9-resumo-dos-arquivos-modificados)
10. [Testando a Integração](#10-testando-a-integração)

---

## 1. Visão Geral

O sistema de **Duplo CPC** (Controle Prévio de Chamada) impede que o operador entre em contato com um cliente que já foi acionado no dia, ou cujo contrato é inválido ou baixado. Ele funciona em duas etapas:

| Etapa | Quando? | O quê? |
|---|---|---|
| **Outbound (Envio)** | Antes de enviar qualquer mensagem 1x1 | O servidor valida o contrato (`validate-contract`) e checa se já existe acionamento no dia (`check-acionamento`). Se qualquer um falhar, **bloqueia o envio**. |
| **Inbound (Recebimento)** | Quando o cliente responde com os 3 últimos dígitos do CPF | O webhook detecta que o texto da resposta bate com o CPF salvo no contato e registra o acionamento digital (`register-acionamento`). |

---

## 2. Diagrama de Fluxo

```mermaid
sequenceDiagram
    actor O as Operador
    participant FE as Frontend (Atendimento.tsx)
    participant WS as WebSocket Gateway
    participant CPC as CpcService
    participant API as API Paschoalotto
    participant WH as WhatsApp (Evolution)
    participant WB as Webhook Service
    
    Note over O,FE: ── FLUXO OUTBOUND (Envio) ──
    O->>FE: Preenche Nome, Telefone, CPF (3 dígitos), Contrato
    FE->>FE: Valida campos (botão só habilita com tudo preenchido)
    FE->>WS: send-message { contactPhone, contactCpf, contactContract, ... }
    
    WS->>CPC: validateContract(cpf, contrato, segmento)
    CPC->>API: GET /validate-contract?cpf=X&contrato=Y&segmento=Z
    API-->>CPC: { sucesso: true/false }
    
    alt Contrato inválido
        CPC-->>WS: false
        WS-->>FE: message-error "Contrato não localizado ou baixado"
        FE-->>O: Toast vermelho de bloqueio (modal permanece aberto)
    end

    WS->>CPC: checkAcionamento(telefone, contrato, segmento)
    CPC->>API: GET /check-acionamento?telefone=X&contrato=Y&segmento=Z
    API-->>CPC: { sucesso: true/false }
    
    alt Já existe CPC hoje
        CPC-->>WS: false
        WS-->>FE: message-error "Já existe CPC hoje"
        FE-->>O: Toast vermelho de bloqueio (modal permanece aberto)
    end

    WS->>WH: Envia mensagem via Evolution API
    WH-->>WS: Confirmação
    WS-->>FE: message-sent (sucesso)
    FE-->>O: Toast verde + modal fecha + conversa aparece

    Note over WH,WB: ── FLUXO INBOUND (Recebimento) ──
    WH->>WB: Webhook message.upsert (cliente respondeu)
    WB->>WB: Texto da mensagem == contact.cpf (3 dígitos)?
    
    alt CPF confere
        WB->>CPC: registerAcionamento(telefone, contrato, segmento)
        CPC->>API: POST /register-acionamento { telefone, contrato, segmento }
        API-->>CPC: { sucesso: true }
        CPC-->>WB: true
        WB->>WB: Log "Acionamento registrado com sucesso"
    end
```

---

## 3. API do Parceiro (Paschoalotto)

### URL Base

```
https://api-cpc.paschoalotto.com.br/GW.BancoBV.WhatsAppCPC/api/WhatsAppCPCGw
```

### Autenticação

Todas as rotas requerem **Basic Auth** no header `Authorization`:

| Usuário | Senha | Base64 |
|---|---|---|
| `Vend` | `vV32nd#s` | `VmVuZDp2VjMybmQjcw==` |

O header completo fica:
```
Authorization: Basic VmVuZDp2VjMybmQjcw==
```

### Rotas

#### 1. `GET /validate-contract`

**Objetivo:** Verifica se o contrato existe e está ativo (não foi baixado) na base do parceiro.

| Parâmetro | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `cpf` | string | ✅ | 3 últimos dígitos do CPF do cliente |
| `contrato` | string | ✅ | Número do contrato |
| `segmento` | string | ✅ | Nome do segmento (ex: "BV Financeira") |

**Resposta de sucesso:**
```json
{ "sucesso": true, "mensagem": "Contrato válido" }
```

**Resposta de falha:**
```json
{ "sucesso": false, "mensagem": "Contrato não localizado ou baixado" }
```

---

#### 2. `GET /check-acionamento`

**Objetivo:** Verifica se já existe um acionamento CPC registrado naquele dia para o telefone/contrato.

| Parâmetro | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `telefone` | string | ✅ | Número de telefone do cliente |
| `contrato` | string | ✅ | Número do contrato |
| `segmento` | string | ✅ | Nome do segmento |

**Resposta — pode contatar:**
```json
{ "sucesso": true, "mensagem": "Nenhum acionamento encontrado hoje" }
```

**Resposta — bloqueado:**
```json
{ "sucesso": false, "mensagem": "Já existe CPC para este contrato" }
```

---

#### 3. `POST /register-acionamento`

**Objetivo:** Registra que o acionamento digital foi efetuado (cliente confirmou identidade).

| Campo (Body JSON) | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `telefone` | string | ✅ | Número de telefone do cliente |
| `contrato` | string | ✅ | Número do contrato |
| `segmento` | string | ✅ | Nome do segmento |

**Resposta de sucesso:**
```json
{ "sucesso": true }
```

---

## 4. Variáveis de Ambiente

Adicionar no arquivo `.env` do backend:

```env
# WhatsApp CPC API
CPC_API_URL="https://api-cpc.paschoalotto.com.br/GW.BancoBV.WhatsAppCPC/api/WhatsAppCPCGw"
CPC_API_AUTH="Basic VmVuZDp2VjMybmQjcw=="
```

> ⚠️ Se essas variáveis **não existirem**, o sistema assume os valores padrão hardcoded como fallback.

---

## 5. Backend — Módulo CPC

### Estrutura de Arquivos

```
backend/src/cpc/
├── cpc.module.ts     ← Módulo NestJS
└── cpc.service.ts    ← Serviço com chamadas HTTP
```

### `cpc.module.ts`

```typescript
import { Module } from '@nestjs/common';
import { CpcService } from './cpc.service';

@Module({
    providers: [CpcService],
    exports: [CpcService]       // Exportar para ser usado em outros módulos
})
export class CpcModule { }
```

### `cpc.service.ts`

```typescript
import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';

@Injectable()
export class CpcService {
    private readonly logger = new Logger(CpcService.name);
    private readonly baseUrl = process.env.CPC_API_URL
        || 'https://api-cpc.paschoalotto.com.br/GW.BancoBV.WhatsAppCPC/api/WhatsAppCPCGw';
    private readonly authHeader = process.env.CPC_API_AUTH
        || 'Basic VmVuZDp2VjMybmQjcw==';

    /**
     * Valida se o contrato existe na base (diferente de baixado).
     */
    async validateContract(cpf: string, contrato: string, segmento: string): Promise<boolean> {
        try {
            const response = await axios.get(`${this.baseUrl}/validate-contract`, {
                params: { cpf, contrato, segmento },
                headers: { Authorization: this.authHeader },
                timeout: 5000
            });
            return response.data?.sucesso === true;
        } catch (error: any) {
            this.logger.error(`Erro ao validar contrato: ${error.message}`);
            return false; // Negar por segurança
        }
    }

    /**
     * Checa se já existe um acionamento CPC na data atual.
     * Retorna true se NÃO existe (pode contatar), false se JÁ existe (bloqueado).
     */
    async checkAcionamento(telefone: string, contrato: string, segmento: string): Promise<boolean> {
        try {
            const response = await axios.get(`${this.baseUrl}/check-acionamento`, {
                params: { telefone, contrato, segmento },
                headers: { Authorization: this.authHeader },
                timeout: 5000
            });
            return response.data?.sucesso === true;
        } catch (error: any) {
            this.logger.error(`Erro ao checar acionamento: ${error.message}`);
            return false; // Negar por segurança
        }
    }

    /**
     * Registra o acionamento digital (após cliente confirmar CPF).
     */
    async registerAcionamento(telefone: string, contrato: string, segmento: string): Promise<boolean> {
        try {
            const response = await axios.post(`${this.baseUrl}/register-acionamento`,
                { telefone, contrato, segmento },
                {
                    headers: { Authorization: this.authHeader },
                    timeout: 8000
                }
            );
            return response.data?.sucesso === true;
        } catch (error: any) {
            this.logger.error(`Erro ao registrar acionamento: ${error.message}`);
            return false;
        }
    }
}
```

### Registrando o Módulo

Em cada módulo que precise usar o `CpcService`, importar o `CpcModule`:

**`websocket.module.ts`:**
```typescript
import { CpcModule } from '../cpc/cpc.module';

@Module({
  imports: [
    // ... outros módulos
    CpcModule,  // ← Adicionar aqui
  ],
})
export class WebsocketModule { }
```

**`webhooks.module.ts`:**
```typescript
import { CpcModule } from '../cpc/cpc.module';

@Module({
  imports: [
    // ... outros módulos
    CpcModule,  // ← Adicionar aqui
  ],
})
export class WebhooksModule { }
```

---

## 6. Backend — Bloqueio Outbound (WebSocket Gateway)

### Arquivo: `websocket.gateway.ts`

#### Passo 1: Injetar no construtor

```typescript
import { CpcService } from '../cpc/cpc.service';

constructor(
    // ... outras dependências
    private cpcService: CpcService,
) { }
```

#### Passo 2: Validar ANTES de alocar linha

Dentro de `handleSendMessage`, **logo após o log inicial** e **ANTES** de checar linhas, inserir:

```typescript
// IMPORTANTE: Deve ocorrer ANTES da atribuição de linha
const isGroup = data.contactPhone?.includes("@g.us") || false;

if (!isGroup) {
    // 1. Resolver o nome do segmento (a API espera nome, não ID)
    let segmentName = 'Default';
    if (user.segment) {
        const segmentObj = await this.prisma.segment.findUnique({
            where: { id: user.segment }
        });
        if (segmentObj) segmentName = segmentObj.name;
    }

    // 2. Buscar CPF e Contrato (do formulário ou do banco)
    const contactCheck = await this.prisma.contact.findFirst({
        where: { phone: data.contactPhone }
    });
    const cpfToValidate = data.contactCpf || contactCheck?.cpf || '';
    const contractToValidate = data.contactContract || contactCheck?.contract || '';

    if (!cpfToValidate || !contractToValidate) {
        return { error: "CPF (3 dígitos) e Contrato são obrigatórios." };
    }

    // 3. Chamar validate-contract
    const isContractValid = await this.cpcService.validateContract(
        cpfToValidate, contractToValidate, segmentName
    );
    if (!isContractValid) {
        client.emit("message-error", {
            error: "Contrato não localizado ou baixado na API CPC."
        });
        return { error: "Contrato não localizado ou baixado na API CPC." };
    }

    // 4. Chamar check-acionamento
    const isAcionamentoOk = await this.cpcService.checkAcionamento(
        data.contactPhone, contractToValidate, segmentName
    );
    if (!isAcionamentoOk) {
        client.emit("message-error", {
            error: "Já existe CPC hoje para este cliente."
        });
        return { error: "Já existe CPC para este contrato e telefone hoje." };
    }
}

// ← Só depois daqui continua com verificação de linha, Evolution, etc.
```

> **Ponto-chave:** A validação CPC ocorre **antes** de buscar/alocar linha do operador. Se o CPC negar, nenhuma linha é alocada e o operador recebe o erro imediatamente.

---

## 7. Backend — Registro Inbound (Webhooks)

### Arquivo: `webhooks.service.ts`

#### Passo 1: Injetar no construtor

```typescript
import { CpcService } from '../cpc/cpc.service';

constructor(
    // ... outras dependências
    private cpcService: CpcService,
) { }
```

#### Passo 2: Detectar confirmação de CPF

Dentro de `handleEvolutionMessage`, logo após `registerClientResponse(from)`:

```typescript
// Registrar resposta do cliente (reseta repescagem)
if (!isGroup) {
    await this.controlPanelService.registerClientResponse(from);

    // --- INTEGRAÇÃO CPC INBOUND ---
    // Se o contato tem CPF (3 dígitos) e contrato salvos,
    // e a mensagem é exatamente os 3 dígitos, registrar acionamento
    if (contact.cpf && contact.cpf.length === 3 && contact.contract) {
        const cleanMsg = messageText.trim();
        if (cleanMsg === contact.cpf) {
            console.log(`✅ [CPC] Cliente confirmou CPF (${contact.cpf})`);

            let segmentName = 'Default';
            if (line.segment) {
                const segObj = await this.prisma.segment.findUnique({
                    where: { id: line.segment }
                });
                if (segObj) segmentName = segObj.name;
            }

            const ok = await this.cpcService.registerAcionamento(
                contactIdentifier, contact.contract, segmentName
            );
            if (ok) {
                console.log(`✅ [CPC] Acionamento registrado com sucesso`);
            } else {
                console.warn(`⚠️ [CPC] Falha ao registrar acionamento`);
            }
        }
    }
    // --------------------------------
}
```

---

## 8. Frontend — Modal de Nova Conversa 1x1

### Arquivo: `Atendimento.tsx`

### Campos obrigatórios no modal

| Campo | Label | Regra |
|---|---|---|
| Nome | `Nome *` | Texto não-vazio |
| Telefone | `Telefone *` | Texto não-vazio |
| CPF | `Últimos 3 dígitos do CPF *` | Exatamente 3 dígitos numéricos |
| Contrato | `Contrato *` | Texto não-vazio |

### Input do CPF (aceita só números, máximo 3)

```tsx
<Label htmlFor="cpf">Últimos 3 dígitos do CPF *</Label>
<Input
    id="cpf"
    placeholder="Ex: 123"
    maxLength={3}
    value={newContactCpf}
    onChange={(e) => {
        const val = e.target.value.replace(/\D/g, '').slice(0, 3);
        setNewContactCpf(val);
    }}
    disabled={isCreatingConversation}
/>
```

### Botão "Enviar" — desabilitado até todos os campos estarem válidos

```tsx
<Button
    onClick={handleNewConversation}
    disabled={
        isCreatingConversation ||
        !newContactName.trim() ||
        !newContactPhone.trim() ||
        !/^\d{3}$/.test(newContactCpf.trim()) ||
        !newContactContract.trim() ||
        (!selectedTemplate && !segmentAllowsFreeMessage) ||
        (!selectedTemplate && segmentAllowsFreeMessage && !newContactMessage.trim())
    }
>
```

> **O botão fica cinza/inativo** se qualquer campo obrigatório estiver vazio. O operador não consegue clicar sem preencher tudo.

### Fluxo do Modal — Não fechar até sucesso

1. O operador preenche os campos e clica "Enviar".
2. O modal **permanece aberto** mostrando "Validando envio..."
3. O WebSocket envia os dados para o backend.
4. **Se `message-error` retornar:** Toast vermelho aparece + modal continua aberto (dados preservados).
5. **Se `message-sent` retornar:** Toast verde aparece + modal fecha + campos são limpos.

### Handler de validação (`handleNewConversation`)

```typescript
if (!newContactCpf.trim() || !/^\d{3}$/.test(newContactCpf.trim())) {
    toast({
        title: "CPF obrigatório",
        description: "É obrigatório informar os 3 últimos dígitos do CPF",
        variant: "destructive",
    });
    return;
}

if (!newContactContract.trim()) {
    toast({
        title: "Contrato obrigatório",
        description: "É obrigatório informar o número do contrato",
        variant: "destructive",
    });
    return;
}
```

### Listener de erro (`message-error`)

```typescript
useRealtimeSubscription("message-error", (data: any) => {
    if (data?.error) {
        playErrorSound();
        setIsSending(false);
        setIsCreatingConversation(false);  // Liberar botão para tentar de novo

        let title = "Mensagem bloqueada";
        if (data.error.includes("CPC")) title = "Bloqueio de CPC";

        toast({
            title,
            description: data.error,
            variant: "destructive",
            duration: 5000,
        });
    }
});
```

---

## 9. Resumo dos Arquivos Modificados

| Arquivo | O quê foi feito |
|---|---|
| `backend/src/cpc/cpc.service.ts` | **NOVO** — Serviço com 3 métodos HTTP (validate, check, register) |
| `backend/src/cpc/cpc.module.ts` | **NOVO** — Módulo NestJS exportando o CpcService |
| `backend/src/websocket/websocket.module.ts` | Importou `CpcModule` |
| `backend/src/websocket/websocket.gateway.ts` | Injetou `CpcService`, adicionou validação CPC antes de alocar linha |
| `backend/src/webhooks/webhooks.module.ts` | Importou `CpcModule` |
| `backend/src/webhooks/webhooks.service.ts` | Injetou `CpcService`, adicionou detecção inbound de CPF |
| `frontend/src/pages/Atendimento.tsx` | Campo CPF obrigatório (3 dígitos), campo Contrato obrigatório, botão desabilitado até preenchimento, modal não fecha até sucesso |
| `backend/.env` | Adicionou `CPC_API_URL` e `CPC_API_AUTH` |

---

## 10. Testando a Integração

### Teste 1 — Contrato Inválido (Outbound)
1. Abrir modal "Nova Conversa" no frontend.
2. Preencher Nome, Telefone, CPF: `999`, Contrato: `INVALIDO123`.
3. Clicar em enviar.
4. **Esperado:** Toast vermelho `"Contrato não localizado ou baixado na API CPC."` e modal continua aberto.

### Teste 2 — CPC já realizado hoje (Outbound)
1. Enviar uma mensagem para um contrato válido (passando pelo validate-contract).
2. Tentar enviar novamente **no mesmo dia** para o mesmo telefone/contrato.
3. **Esperado:** Toast vermelho `"Já existe CPC hoje para este cliente."`.

### Teste 3 — Registro de acionamento (Inbound)
1. Enviar uma mensagem para um cliente com CPF `223` e contrato `ABC123` preenchidos.
2. Simular o cliente respondendo com o texto exatamente `223`.
3. **Esperado:** No log do backend: `"✅ [Webhook CPC] Acionamento registrado com sucesso"`.

### Verificando os logs
No Docker/terminal do backend, procurar por:
```bash
# Outbound — validação
grep "Validando contrato" logs
grep "Checando acionamento" logs

# Inbound — registro
grep "Webhook CPC" logs
grep "Registrando acionamento" logs
```
