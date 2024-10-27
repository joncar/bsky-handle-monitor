FROM mcr.microsoft.com/devcontainers/typescript-node:1-22-bookworm

COPY src/ /src/
COPY .yarnrc.yml /
COPY package.json /
COPY tsconfig.json /
COPY yarn.lock /

RUN tsc

ENTRYPOINT ["node /dist/index.js"]