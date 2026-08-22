# syntax=docker/dockerfile:1
ARG NODE_VERSION=24.19.0
ARG PNPM_VERSION=11.18.0

FROM node:${NODE_VERSION}-bookworm-slim AS development
ARG PNPM_VERSION
ARG TARGETARCH
ENV PNPM_HOME=/pnpm
ENV PATH=${PNPM_HOME}:${PATH}
RUN corepack enable && corepack prepare pnpm@${PNPM_VERSION} --activate
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
