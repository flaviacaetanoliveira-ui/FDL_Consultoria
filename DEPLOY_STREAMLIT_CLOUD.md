# Deploy no Streamlit Community Cloud

## Arquivo principal do app

| Campo no painel do Streamlit Cloud | Valor |
|-----------------------------------|--------|
| **Main file** | `app_operacional.py` |

O repositório deve apontar para este arquivo como entrada do Streamlit (`streamlit run app_operacional.py`).

---

## O que foi preparado no projeto

1. **`fdl_paths.py`** — diretório base dos dados (`BASE_DIR` / `CLIENTE_BASE_DIR`) sem caminho fixo de Windows.
2. **`requirements.txt`** — dependências do app e do pipeline.
3. **`.streamlit/config.toml`** — configuração mínima do servidor.
4. **`.streamlit/secrets.toml.example`** — modelo de segredo (sem credenciais reais).
5. **`cliente_1/`** — se existir na raiz do repo, torna-se a base padrão (`FDL_BASE_DIR`) para dados originais; senão usa **`data_cliente/`** (vazia no repo, com `.gitkeep`).
6. **`.gitignore`** — ignora `secrets.toml` e artefatos comuns.

Nenhuma regra de negócio, cálculo ou etapa do pipeline foi alterada — apenas a origem do caminho `BASE_DIR`.

---

## Bloqueio crítico: dados no Cloud

O app **lê arquivos CSV/Excel do disco** nas pastas:

- `Vendas - Mercado Livre/`
- `Liberações_ML/`
- `notas_saida/`
- `contas_receber/`

**No Streamlit Community Cloud não existe** o seu OneDrive nem pastas `C:\...`. O servidor só vê:

- o **repositório Git** publicado, e
- **segredos** (`st.secrets`) e variáveis de ambiente configuradas no painel.

### Forma mais simples de contornar no MVP

1. **Colocar uma cópia dos dados dentro do repositório** (por exemplo sob `data_cliente/`, com a mesma estrutura de pastas que você usa no PC).  
   - Limite de tamanho do repo / GitHub: atenção ao volume; para bases grandes, isso não escala.
2. **Definir o segredo `FDL_BASE_DIR`** no painel do Streamlit Cloud **apenas se** no futuro houver suporte a caminho montado ou outra origem — hoje, na prática, o caminho útil no Cloud é **relativo ao diretório do app** (repo).

**Valor padrão sem configuração:** `FDL_BASE_DIR` não definido → se existir `./cliente_1` no repositório (pasta real ou junction para o OneDrive), usa essa; senão `data_cliente/`.

**Desenvolvimento local com `cliente_1` fora do repo:** defina variável de ambiente antes de rodar:

```powershell
$env:FDL_BASE_DIR = "C:\caminho\para\cliente_1"
streamlit run app_operacional.py
```

Ou crie `.streamlit/secrets.toml` (não versionar) com:

```toml
FDL_BASE_DIR = "C:/caminho/para/cliente_1"
```

---

## Passo a passo para publicar (objetivo)

1. Subir o código para um repositório **GitHub** (GitLab/Bitbucket também são aceitos em alguns fluxos; o Cloud usa GitHub).
2. Em [share.streamlit.io](https://share.streamlit.io), **New app** → conectar o repo e branch.
3. **Main file path:** `app_operacional.py`.
4. **Python version:** deixe o padrão do Cloud ou escolha 3.11.x se necessário.
5. **Secrets** (Settings → Secrets): se usar `data_cliente/` dentro do repo, **não é obrigatório** secret de caminho. Se usar outro caminho absoluto no runner (raro), adicione:

   ```toml
   FDL_BASE_DIR = "/mount/path" 
   ```

6. **Deploy** e aguardar build. Se faltar dependência, o log de build mostrará o erro.
7. Testar: login (`operacional_usuarios.py`), tela principal, filtros, tabela, **Atualizar dados** (limpa cache).

---

## Login e senhas

Usuários e senhas em texto estão em `operacional_usuarios.py`. Para produção no Cloud, o ideal é **não** versionar senhas reais: use `st.secrets` ou variáveis de ambiente e leia no código (refino futuro, fora do escopo deste MVP).

---

## Checklist de compatibilidade

| Item | Status |
|------|--------|
| Entry point único | `app_operacional.py` |
| Imports relativos ao repo | `fdl_paths.py` + módulos do projeto |
| Caminhos Windows removidos do código | Sim (substituídos por `fdl_paths`) |
| Dados disponíveis no Cloud | **Sua responsabilidade** — copiar para `data_cliente/` ou equivalente no repo |
| `requirements.txt` | Presente |
| `.streamlit/config.toml` | Presente |

---

## Arquivos criados ou alterados (resumo)

**Criados:** `fdl_paths.py`, `data_cliente/.gitkeep`, `data_cliente/README.md`, `.streamlit/config.toml`, `.streamlit/secrets.toml.example`, `.gitignore`, `DEPLOY_STREAMLIT_CLOUD.md`.

**Alterados:** todos os módulos que tinham `BASE_DIR` / `CLIENTE_BASE_DIR` com caminho absoluto (pipeline e diagnósticos), `requirements.txt`.

**App:** `app_operacional.py` já importava `BASE_DIR` via `etapa4b_integracao_contas_receber` → cadeia passa a usar `fdl_paths` automaticamente.
