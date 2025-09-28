# Spicy Regs API

Spicy Regs API attempts to provide an API to easily download and query data from regulations.gov.

Most of the raw data is stored in an open source project called mirrulations.

Most of the code here attempts to format that raw data into a usable format that can be used by other consumers

## Setup

```sh
# install uv
pip install uv

# install packages in a venv
uv sync

# run main.py
uv main.py
```