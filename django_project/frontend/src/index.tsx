import * as Sentry from "@sentry/react";
import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import './styles/index.scss';
import "./styles/font.css";
import reportWebVitals from './reportWebVitals';

Sentry.init({
  dsn: 'SENTRY_DSN',
  tunnel: '/sentry-proxy/',
  tracesSampleRate: 0.5
});

const root = createRoot(document.getElementById('app')!);
root.render(<App />);
reportWebVitals();
