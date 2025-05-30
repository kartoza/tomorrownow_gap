import React from 'react';
import './styles/App.scss';
import { useGapContext } from './contexts/GapContext';
import {
  Box,
  Button,
  Flex,
  Spacer,
} from '@chakra-ui/react';
import Navbar from './components/Navbar';
import Footer from './components/Footer';
import LandingPage from './pages/LandingPage';

function Home() {
  const gapContext = useGapContext();

  const redirectToURL = (url: string) => {
    window.location.href = url;
  };

  return (
    <Box className="App">
      {/* Navbar */}
      <Navbar/>

      {/* Main content */}
      <LandingPage />
      
      {/* Footer */}
      <Footer />
    </Box>
  );
}

export default Home;
