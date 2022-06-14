FROM node:16.15.1-alpine as builder
WORKDIR /app
COPY . .
RUN npm install --silent
RUN mkdir build && chown -R node /app/node_modules /app/build
USER node
RUN npm run build


FROM node:16.15.1-alpine
USER node
COPY --from=builder /app /app
WORKDIR /app
EXPOSE 3000
CMD [ "npm", "run","dev"]
