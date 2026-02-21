import { Test, TestingModule } from '@nestjs/testing';
import { CpcService } from './cpc.service';

describe('CpcService', () => {
  let service: CpcService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [CpcService],
    }).compile();

    service = module.get<CpcService>(CpcService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
