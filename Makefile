.PHONY: install build typecheck lint format format-check test coverage event-loop-stress check clean docker-build

install:
	corepack pnpm install --frozen-lockfile

build:
	corepack pnpm build

typecheck:
	corepack pnpm typecheck

lint:
	corepack pnpm lint

format:
	corepack pnpm format

format-check:
	corepack pnpm format:check

test:
	corepack pnpm test

coverage:
	corepack pnpm test:coverage

event-loop-stress:
	corepack pnpm test:event-loop-stress

check:
	corepack pnpm check

clean:
	corepack pnpm clean

docker-build:
	docker build --target test --tag tsservicelib-test .
