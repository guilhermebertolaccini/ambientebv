import { Injectable, Logger, HttpException, HttpStatus } from '@nestjs/common';
import axios from 'axios';

@Injectable()
export class CpcService {
    private readonly logger = new Logger(CpcService.name);
    private readonly baseUrl = process.env.CPC_API_URL || 'https://api-cpc.paschoalotto.com.br/GW.BancoBV.WhatsAppCPC/api/WhatsAppCPCGw';
    private readonly authHeader = process.env.CPC_API_AUTH || 'Basic VmVuZDp2VjMybmQjcw=='; // Base64 de "Vend:vV32nd#s"
    private readonly defaultHeaders = {
        'Authorization': this.authHeader,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    };

    /**
     * Valida se o contrato existe na base (diferente de baixado).
     * @param cpf Os 3 últimos dígitos do CPF
     * @param contrato Número do contrato
     * @param segmento Nome do segmento
     */
    async validateContract(cpf: string, contrato: string, segmento: string): Promise<boolean> {
        try {
            this.logger.debug(`Validando contrato: CPF ${cpf}, Contrato ${contrato}, Segmento ${segmento}`);

            const response = await axios.get(`${this.baseUrl}/validate-contract`, {
                params: {
                    cpf,
                    contrato,
                    segmento
                },
                headers: this.defaultHeaders,
                timeout: 5000 // 5 seconds timeout
            });

            // A API retorna { sucesso: true/false, mensagem: string }
            return response.data?.sucesso === true;
        } catch (error: any) {
            if (error.response?.data) {
                this.logger.warn(`Falha na validação do contrato (API GW): ${JSON.stringify(error.response.data)}`);
                return false;
            }
            this.logger.error(`Erro ao validar contrato na API CPC: ${error.message}`);
            return false; // Em caso de erro de rede, negamos por segurança ou conforme regra de negócio
        }
    }

    /**
     * Valida se já existe um acionamento CPC na data atual no telefone pertencente ao contrato passados na requisição.
     * @param telefone Número de telefone
     * @param contrato Número do contrato
     * @param segmento Nome do segmento
     */
    async checkAcionamento(telefone: string, contrato: string, segmento: string): Promise<boolean> {
        try {
            this.logger.debug(`Checando acionamento: Telefone ${telefone}, Contrato ${contrato}, Segmento ${segmento}`);

            const response = await axios.get(`${this.baseUrl}/check-acionamento`, {
                params: {
                    telefone,
                    contrato,
                    segmento
                },
                headers: this.defaultHeaders,
                timeout: 5000
            });

            // Retorno esperado: { sucesso: true } significa que NÃO existe CPC para este contrato hoje.
            // Se sucesso for false, significa que já existe CPC (ou outro erro)
            return response.data?.sucesso === true;
        } catch (error: any) {
            if (error.response?.data) {
                this.logger.warn(`Falha no check-acionamento (API GW): ${JSON.stringify(error.response.data)}`);
                return false; // Se false, bloquear envio!
            }
            this.logger.error(`Erro ao checar acionamento na API CPC: ${error.message}`);
            return false;
        }
    }

    /**
     * Insere na base o acionamento digital.
     * @param telefone Número de telefone
     * @param contrato Número do contrato
     * @param segmento Nome do segmento
     */
    async registerAcionamento(telefone: string, contrato: string, segmento: string): Promise<boolean> {
        try {
            this.logger.log(`Registrando acionamento: Telefone ${telefone}, Contrato ${contrato}, Segmento ${segmento}`);

            const response = await axios.post(`${this.baseUrl}/register-acionamento`, {
                telefone,
                contrato,
                segmento
            }, {
                headers: {
                    ...this.defaultHeaders,
                    'Content-Type': 'application/json'
                },
                timeout: 8000
            });

            return response.data?.sucesso === true;
        } catch (error: any) {
            this.logger.error(`Erro ao registrar acionamento na API CPC: ${error.message}`);
            return false;
        }
    }
}
