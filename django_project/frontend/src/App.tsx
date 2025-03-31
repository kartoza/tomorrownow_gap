import React from 'react';
import { ChakraProvider, defaultSystem } from '@chakra-ui/react';
import { GapContextProvider } from './contexts/GapContext';
import ErrorBoundary from './components/ErrorBoundary';
import Home from './Home';

const App = () => (
  <ChakraProvider value={defaultSystem}>
    <GapContextProvider>
      <ErrorBoundary>
        <Home />
      </ErrorBoundary>
    </GapContextProvider>
  </ChakraProvider>
);

export default App;
