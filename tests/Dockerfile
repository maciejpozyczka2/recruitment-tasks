FROM node:20-slim

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY tsconfig.json jest.config.js ./
COPY src ./src
COPY __tests__ ./__tests__

CMD ["npm", "test"]
