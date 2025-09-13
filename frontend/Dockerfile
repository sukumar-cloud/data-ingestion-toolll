# Build stage
FROM node:18-alpine AS build
WORKDIR /app
COPY . .
# No build step needed as we're serving static files directly

# Run stage
FROM nginx:alpine
COPY --from=build /app /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
