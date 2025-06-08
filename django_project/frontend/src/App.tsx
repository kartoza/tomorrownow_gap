import React from 'react';
import { ChakraProvider } from '@chakra-ui/react';
import { GapContextProvider } from './context/GapContext';
import { ScrollProvider } from './context/ScrollContext';
import ErrorBoundary from './components/ErrorBoundary';
import store from './app/store';
import system from './theme';
import { Provider } from 'react-redux';
import { RouterProvider } from 'react-router-dom';
import {router} from './app/router';
import { Toaster } from './components/ui/toaster';

const App = () => (
  <React.StrictMode>
    <ChakraProvider value={system}>
      <Provider store={store}>
        <GapContextProvider>
          <ScrollProvider>
            <ErrorBoundary>
              <RouterProvider router={router} />
              <Toaster />
            </ErrorBoundary>
          </ScrollProvider>
        </GapContextProvider>
      </Provider>
    </ChakraProvider>
  </React.StrictMode>
);

export default App;
