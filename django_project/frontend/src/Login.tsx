// src/Signup.tsx
import React from 'react';
import { createRoot } from 'react-dom/client';
import { ChakraProvider, defaultSystem } from '@chakra-ui/react';
import { LoginAccountForm } from './pages/Login';
import { GapContextProvider } from './contexts/GapContext';
import ErrorBoundary from './components/ErrorBoundary';
import { Toaster } from 'react-hot-toast';

const root = createRoot(document.getElementById('app')!);

root.render(
  <ChakraProvider value={defaultSystem}>
    <GapContextProvider>
      <ErrorBoundary>
        <LoginAccountForm />
        <Toaster
          position="top-center"
          reverseOrder={false}
          toastOptions={{
            duration: 3000,
            style: {
              background: '#1a202c',
              color: '#fff',
            },
          }}
        />
      </ErrorBoundary>
    </GapContextProvider>
  </ChakraProvider>
);
