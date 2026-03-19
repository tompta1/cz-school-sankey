FROM node:22-bookworm-slim

# Python 3 is needed for the ETL script
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip python3-openpyxl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json .
RUN npm install

COPY . .

# If the raw MŠMT xlsx is present, parse + build; otherwise fall back to demo.
RUN if [ -f etl/data/raw/2025/msmt_2025_raw.xlsx ]; then \
      npm run etl:full; \
    else \
      npm run etl:demo; \
    fi

EXPOSE 5173

# Vite dev server bound to 0.0.0.0 so the host can reach it
CMD ["npx", "vite", "--host", "0.0.0.0"]
