# Como capturar o motivo da não-renderização do termômetro

## 1. Rodar o Streamlit com debug ligado

### Windows (PowerShell)

```powershell
$env:FDL_RG_PACE_DEBUG="1"
streamlit run app_operacional.py
```

### Windows (CMD)

```cmd
set FDL_RG_PACE_DEBUG=1
streamlit run app_operacional.py
```

### Linux/Mac

```bash
FDL_RG_PACE_DEBUG=1 streamlit run app_operacional.py
```

Modo **admin** (`FDL_APP_MODE=admin` no ambiente ou secrets) também exibe as mensagens de debug, sem precisar da variável.

## 2. Navegar ao cenário

- Empresa: **Gama Home** (ou a desejada)
- Período: **01/04/2026** a **30/04/2026** (mês civil cheio, se for o caso de teste)

## 3. Procurar captions

Linhas iniciadas com **`🔍 pace debug:`**, **`🔍 pace pré-render:`** e **`🔍 pace pós-render:`** devem aparecer **entre** o expander **Cobertura comercial** e os **KPIs de topo**.

## 4. Copiar o texto completo e reportar

Cole no chat/issue o texto literal de cada caption para diagnóstico (integração vs componente CSS/HTML).

## Script sem Streamlit

Para validar só o cálculo (sem UI):

```powershell
python scripts/debug_pace_gama_abril.py
```

## Renderização isolada do componente

Para ver só o HTML/CSS do termômetro:

```powershell
streamlit run scripts/debug_render_termometro.py
```
