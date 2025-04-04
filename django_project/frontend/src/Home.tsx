import React from 'react';
import './styles/App.scss';
import { useGapContext } from './contexts/GapContext';
import {
  Box,
  Button,
  Flex,
  Spacer,
} from '@chakra-ui/react';

function Home() {
  const gapContext = useGapContext();

  const redirectToURL = (url: string) => {
    window.location.href = url;
  };

  return (
    <Box className="App">
      {/* Navbar with just Sign Up */}
      <Flex as="nav" p={4} bg="gray.900" color="white" align="center">
        <Spacer />
        <a href="/signup/">
          <Button colorScheme="purple" size="sm">
            Sign Up
          </Button>
        </a>
      </Flex>

      {/* Main content */}
      <Box className="App-header">
        <p>OSIRIS II Global Access Platform</p>

        <div className="button-container">
          <div
            className="App-link link-button"
            onClick={() => redirectToURL(gapContext.api_swagger_url)}
          >
            API Swagger Docs
          </div>
          <div
            className="App-link link-button"
            onClick={() => redirectToURL(gapContext.api_docs_url)}
          >
            API Documentation
          </div>
        </div>
      </Box>
    </Box>
  );
}

export default Home;
