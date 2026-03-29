# `cliente_1` — cópia local / exemplo no repositório

**Produção (Anto Moveis):** defina em `.streamlit/secrets.toml` o **`FDL_BASE_DIR`** com o caminho absoluto da base real no OneDrive, por exemplo  
`.../OneDrive - FDL Consultoria/Cursor/Anto Moveis/cliente_1`. Assim Repasse e Frete leem a mesma pasta que os dados operacionais — **não** esta pasta de exemplo dentro do V2.

Se esta pasta **existir** na raiz do V2 **e** `FDL_BASE_DIR` **não** estiver definido (secrets nem variável de ambiente), o código usa-a como fallback (`fdl_paths.py`), em vez de `data_cliente/`.

## Opções

1. **Sincronizar / copiar** a pasta real do OneDrive para aqui (mesma estrutura).
2. **Atalho (symlink)** no Windows (PowerShell, como administrador se necessário):

   ```powershell
   New-Item -ItemType Junction -Path "C:\...\V2\cliente_1" -Target "C:\...\Anto Moveis\cliente_1"
   ```

3. **Não usar esta pasta**: apague-a ou renomeie — o projeto volta a usar `data_cliente/` por defeito.

## Estrutura esperada (igual ao pipeline)

- `Vendas - Mercado Livre/` — exports ML (.xlsx / .csv) para vendas e Frete
- `Liberações_ML/`, `notas_saida/`, `contas_receber/` — quando correr o pipeline completo
- `precomputed/` *(opcional)* — pode guardar aqui o `.csv`/`.xlsx` de **Repasse** e apontar `FDL_PRECOMPUTED_PATH` para esse ficheiro (caminho absoluto ou relativo ao repo, conforme `app_operacional`)

Repasse na Cloud continua com `FDL_PRECOMPUTED_URL`; Frete remoto com `FDL_FRETE_VENDAS_URL`.

**Não versionar** ficheiros de dados reais (ver `.gitignore`).
