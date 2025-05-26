// src/Signup.tsx
import React from 'react';
import { createRoot } from 'react-dom/client';
import { ChakraProvider, defaultSystem, useDisclosure } from '@chakra-ui/react';
import { BrowserRouter } from 'react-router-dom';
import SignIn from './pages/Login';
import { GapContextProvider } from './contexts/GapContext';
import ErrorBoundary from './components/ErrorBoundary';
import store from './store';
import { Provider } from 'react-redux';

const root = createRoot(document.getElementById('app')!);

function App() {
  const { open, onOpen, onClose } = useDisclosure();

  return (
    <ChakraProvider value={defaultSystem}>
      <Provider store={store}>
        <BrowserRouter>
          <GapContextProvider>
            <ErrorBoundary>
              <SignIn isOpen={open} onClose={onClose} />
              <button onClick={onOpen}>Open Login</button>
            </ErrorBoundary>
          </GapContextProvider>
        </BrowserRouter>
      </Provider>
    </ChakraProvider>
  );
}

root.render(<App />);
