// src/Signup.tsx
import React from 'react';
import { createRoot } from 'react-dom/client';
import { ChakraProvider, defaultSystem } from '@chakra-ui/react';
import { SignupAccountForm } from './pages/SignupAccount';
import { GapContextProvider } from './contexts/GapContext';
import ErrorBoundary from './components/ErrorBoundary';

const root = createRoot(document.getElementById('app')!);

root.render(
  <ChakraProvider value={defaultSystem}>
    <GapContextProvider>
      <ErrorBoundary>
        <SignupAccountForm />
      </ErrorBoundary>
    </GapContextProvider>
  </ChakraProvider>
);
