#!/bin/sh

# Script to run the Django project with a UI bundle
# Useful if you want to expose local development server to the internet using VSCode dev tunnel or similar tools
# Note: no hot-reloading of the frontend is available in this mode

# Build frontend
cd /home/web/project/django_project/frontend
echo "generating theme types"
npx -y @chakra-ui/cli typegen ./src/theme.ts
echo "npm build"
npm run build

# Collect static files
cd /home/web/project/django_project
echo "Collecting static files..."
python manage.py collectstatic --noinput --clear
echo "Static files collected."

# This is pre-launch script for Launch Configuration: Django: Run with UI Bundle

# Dev tunnel configuration:
# - install dev tunnel cli: https://learn.microsoft.com/en-gb/azure/developer/dev-tunnels/get-started?tabs=linux
# - login with your github/Microsoft account: ./devtunnel user login
# - run dev tunnel: ./devtunnel host --p 8000
# - or allow anonymous access: ./devtunnel host --p 8000 --allow-anonymous
