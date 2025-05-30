import React from 'react';
import { ChakraProvider, defaultSystem } from '@chakra-ui/react';
import { GapContextProvider } from './contexts/GapContext';
import ErrorBoundary from './components/ErrorBoundary';
import Home from './Home';
import store from './store';
import { Provider } from 'react-redux';
import { BrowserRouter } from 'react-router-dom';

const App = () => (
  <ChakraProvider value={defaultSystem}>
    <Provider store={store}>
      <BrowserRouter>
      <GapContextProvider>
        <ErrorBoundary>
          <Home />
        </ErrorBoundary>
      </GapContextProvider>
      </BrowserRouter>
    </Provider>
  </ChakraProvider>
);

export default App;
