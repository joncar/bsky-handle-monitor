FROM mcr.microsoft.com/devcontainers/typescript-node:1-22-bookworm

COPY .yarnrc.yml /
COPY /.yarn/ /.yarn/
COPY package.json /
COPY src/ /src/
COPY tsconfig.json /
COPY yarn.lock /

RUN yarn install
RUN tsc

ENTRYPOINT ["node /dist/index.js"]