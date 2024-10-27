FROM mcr.microsoft.com/devcontainers/typescript-node:1-22-bookworm

WORKDIR /app

COPY .yarnrc.yml ./
COPY .yarn/ ./.yarn/
COPY package.json ./
COPY tsconfig.json ./
COPY yarn.lock ./
RUN yarn install

COPY src/ ./src/
RUN tsc

ENTRYPOINT ["node", "/app/dist/index.js", "--db", "/app/data/handles.db"]