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

## Por que o termômetro pode não aparecer em dev?

Se o **relógio da máquina** não estiver **no mesmo mês civil** do período filtrado (por exemplo filtro abril/2026 mas `date.today()` em março/2026), o modo **mês corrente** não calcula pace e o termômetro não renderiza — comportamento correto por design (“regra do relógio”).

### Como verificar a data do servidor

```powershell
python -c "from datetime import date; print(date.today())"
```

Se o resultado for, por exemplo, `2026-03-19` e você filtrar **01/04/2026 a 30/04/2026**, o termômetro pode corretamente não aparecer.

### Como ver o mesmo texto que o caption admin mostraria (sem Streamlit)

```powershell
python scripts/inspect_pace_debug_caption.py
```

### Como testar `mes_corrente` sem mudar o relógio

1. Rode `scripts/debug_pace_abril_corrente.py`, que usa **hoje fixo** para o cenário desejado, ou
2. Escreva um teste que chama ``compute_pace_mensal(..., hoje=date(...))``.
