// src/Signup.tsx
import React from 'react';
import { createRoot } from 'react-dom/client';
import { ChakraProvider, defaultSystem } from '@chakra-ui/react';
import { LoginAccountForm } from './pages/Login';
import { GapContextProvider } from './contexts/GapContext';
import ErrorBoundary from './components/ErrorBoundary';

const root = createRoot(document.getElementById('app')!);

root.render(
  <ChakraProvider value={defaultSystem}>
    <GapContextProvider>
      <ErrorBoundary>
        <LoginAccountForm />
      </ErrorBoundary>
    </GapContextProvider>
  </ChakraProvider>
);
