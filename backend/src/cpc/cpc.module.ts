import { Module } from '@nestjs/common';

@Module({
    providers: [CpcService],
    exports: [CpcService]
})
export class CpcModule { }
