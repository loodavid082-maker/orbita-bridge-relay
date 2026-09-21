FROM node:24-alpine
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev=false
COPY server.js ./
ENV NODE_ENV=production
CMD ["node","server.js"]
