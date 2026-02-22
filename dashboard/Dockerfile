FROM nginx:alpine

# Copy custom Nginx configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy static frontend files to Nginx serve directory
COPY *.html /usr/share/nginx/html/
