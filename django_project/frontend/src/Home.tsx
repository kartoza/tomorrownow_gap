import React, { useEffect, useState } from 'react';
import './styles/App.scss';
import { useGapContext } from './contexts/GapContext';
import { Box } from '@chakra-ui/react';
import Navbar from './components/Navbar';
import Footer from './components/Footer';
import LandingPage from './pages/LandingPage';
import { SignupRequest } from './pages/SignupRequest';
import GlobalAccessPlatform from './pages/LandingPage/Dashboard';

function Home() {
  const [showRequestDialog, setShowRequestDialog] = useState(false);
  const [user, setUser] = useState(null);

  useEffect(() => {
    fetch('/api/me/', { credentials: 'include' })
      .then((res) => {
        if (!res.ok) throw new Error('Not authenticated');
        return res.json();
      })
      .then((data) => {
        if (data.email_verified) {
          // Now check if the user has already submitted a request
          fetch('/api/signup-request/me/', { credentials: 'include' })
            .then((res) => {
              if (res.status === 404) {
                setUser(data);
                setShowRequestDialog(true); // Only show if no request exists
              }
            })
            .catch(() => {
              // Ignore if the check fails
            });
        }
      })
      .catch(() => {
        // ignore if not logged in
      });
  }, []);
  
  
  return (
    <Box className="App">
      {/* Navbar */}
      {/* <Navbar/> */}

      {/* Main content */}
      {/* <LandingPage /> */}
      <GlobalAccessPlatform />

      {user && (
        <SignupRequest
          user={user}
          isOpen={showRequestDialog}
          onClose={() => setShowRequestDialog(false)}
        />
      )}
      
      {/* Footer */}
      {/* <Footer /> */}
    </Box>
  );
}

export default Home;
