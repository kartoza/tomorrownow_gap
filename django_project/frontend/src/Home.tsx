import React from 'react';
import './styles/App.scss';
import { useGapContext } from './contexts/GapContext';
import { SignupAccountForm } from './pages/SignupAccount';
import {
  Box,
  Button,
  useDisclosure
} from '@chakra-ui/react';
import {
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalCloseButton,
  ModalBody,
} from '@chakra-ui/modal';

function Home() {
  const gapContext = useGapContext();
  const { open, onOpen, onClose } = useDisclosure();

  const redirectToURL = (url: string) => {
    window.location.href = url;
  };

  return (
    <div className="App">
      <header className="App-header">
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

        <div className="button-container">
          <Button colorScheme="purple" mt={4} onClick={onOpen}>
            Sign Up
          </Button>
        </div>
      </header>

      {/* Signup Form Modal */}
      <Modal isOpen={open} onClose={onClose} size="lg">
        <ModalOverlay />
        <ModalContent>
          <ModalHeader textAlign="center" fontSize="xl">
            Create Your Account
          </ModalHeader>
          <ModalCloseButton />
          <ModalBody pb={6}>
            <SignupAccountForm />
          </ModalBody>
        </ModalContent>
      </Modal>
    </div>
  );
}

export default Home;
