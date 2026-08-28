ARG DEPENDENCY_DOCKER_REGISTRY=docker.io
ARG NODE_VERSION=24.19.0
ARG PNPM_VERSION=11.18.0

FROM ${DEPENDENCY_DOCKER_REGISTRY}/library/node:${NODE_VERSION}-bookworm-slim AS development
ARG PNPM_VERSION
ARG TARGETARCH
ARG NPM_CONFIG_REGISTRY=https://registry.npmjs.org/
ENV PNPM_HOME=/pnpm
ENV PATH=${PNPM_HOME}:${PATH}
RUN corepack enable \
    && corepack prepare pnpm@${PNPM_VERSION} --activate \
    && pnpm config set registry "${NPM_CONFIG_REGISTRY}"
WORKDIR /workspace

FROM development AS dependencies
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN --mount=type=cache,id=servicegen-typescript-pnpm-${PNPM_VERSION}-${TARGETARCH},target=/pnpm/store,sharing=locked \
    pnpm config set store-dir /pnpm/store \
    && pnpm install --frozen-lockfile

FROM dependencies AS build
COPY . .
RUN pnpm build

FROM build AS test
RUN pnpm check
