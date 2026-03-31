# Operação: materialização financeira (repasse, frete, faturamento)

## Fluxo

1. **OneDrive (ou pasta local):** fontes de dados (vendas, liberações, pedidos, custo, JSON de parâmetros).
2. **Tarefa agendada ou execução manual:** `agendamento\run_materialize_financeiro.bat` ou `python processing\materialize_financeiro.py`.
3. **Saída:** `data_products\<cliente>\<empresa>\<modulo>\current\` com `dataset*.parquet`, CSV espelho e `metadata.json`.
4. **Streamlit:** em modo materializado, lê apenas os ficheiros finais em `current/` (sem recomputar o pipeline).

**Faturamento:** o **padrão** é params `schema_version` 2 e saída única em `data_products\<cliente_slug>\faturamento\current\`. O layout antigo por empresa (`\<cliente\>\<empresa>\faturamento\current\` com params V1) é **legado deprecado** — ver [faturamento_pipeline.md](faturamento_pipeline.md).

**Repasse (pipeline, variáveis, consumo no app):** ver [modulo_repasse_estrutura.md](modulo_repasse_estrutura.md).

## Lock

`processing\materialize_financeiro.py` adquire um lock em `agendamento\locks\materialize_financeiro.lock`. Se existir e tiver menos de 2 h, a segunda execução falha com código `2`. Locks mais velhos são removidos (órfãos).

Para testes: `--no-lock`.

## Escrita atômica

Parquet, CSV e `metadata.json` são escritos primeiro como ficheiros `.tmp` no mesmo diretório e depois substituídos com `os.replace`, reduzindo leitura de ficheiros a meio.

## Logs

- `agendamento\logs\materialize_financeiro.log` — runner dedicado.
- `agendamento\logs\rotina.log` — rotina completa (`run_rotina_dados.bat`), se usada.

Cada execução regista início, fim e código de saída.

## Execução manual (exemplo)

```powershell
cd C:\Users\...\V2
$env:FDL_BASE_DIR = "C:\dados\cliente_1"
$env:FDL_FATURAMENTO_PARAMS = "C:\dados\faturamento_params.json"
python processing\materialize_financeiro.py --modulo all --cliente meu_cliente --empresa minha_empresa
```

## Task Scheduler (Windows)

| Campo | Valor |
|--------|--------|
| Programa | `cmd.exe` |
| Argumentos | `/c "C:\caminho\absoluto\para\V2\agendamento\run_materialize_financeiro.bat"` |
| Iniciar em | `C:\caminho\absoluto\para\V2` |
| Variáveis de ambiente da tarefa | `FDL_BASE_DIR`, `FDL_FATURAMENTO_PARAMS` (opcional), `FDL_MATERIALIZE_CLIENTE`, `FDL_MATERIALIZE_EMPRESA` |

Use **caminhos absolutos** nas variáveis para evitar falhas com o diretório de trabalho.

## Como validar sucesso

- Exit code `0` no log.
- `metadata.json` em `faturamento\current` com `generated_at` recente e `row_count` > 0.
- Timestamps dos três ficheiros coerentes.
