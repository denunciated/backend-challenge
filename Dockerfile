FROM oven/bun:1.3.3
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

COPY . .
USER bun
EXPOSE 3000

CMD [ "bun", "src/main.ts" ]