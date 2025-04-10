import React, { useState, useEffect } from 'react';
import {
  Box,
  Button,
  Input,
  Textarea,
  Heading
} from '@chakra-ui/react';
import { FormControl, FormLabel } from '@chakra-ui/form-control';
import { Stack } from '@chakra-ui/layout';

interface SignupRequestFormProps {
  user: {
    email: string;
    first_name?: string;
    last_name?: string;
  };
}

const SignupRequestForm = ({ user }: SignupRequestFormProps) => {
  const [formData, setFormData] = useState({
    first_name: user.first_name || '',
    last_name: user.last_name || '',
    email: user.email,
    description: '',
  });

  const [loading, setLoading] = useState(false);
  const [successMessage, setSuccessMessage] = useState('');
  const [errorMessage, setErrorMessage] = useState('');

  useEffect(() => {
    const saved = sessionStorage.getItem('signup-request');
    if (saved) {
      try {
        setFormData(JSON.parse(saved));
      } catch (e) {
        console.warn("Could not parse saved form data", e);
      }
    }
  }, []);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const uid = params.get("uid");
  
    if (uid) {
      fetch(`/api/user-uid/${uid}/`)
        .then((res) => {
          if (!res.ok) throw new Error('User fetch failed');
          return res.json();
        })
        .then((data) => {
          setFormData(prev => ({
            ...prev,
            email: data.email || '',
            first_name: data.first_name || '',
            last_name: data.last_name || ''
          }));
        })
        .catch((err) => {
          console.error("Error loading user from uid:", err);
          setErrorMessage("Could not preload user information.");
        });
    }
  }, []);

  useEffect(() => {
    sessionStorage.setItem('signup-request', JSON.stringify(formData));
  }, [formData]);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setSuccessMessage('');
    setErrorMessage('');

    try {
      const response = await fetch('/api/signup-request/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': getCookie('csrftoken'),
        },
        credentials: 'include',
        body: JSON.stringify(formData),
      });

      if (response.status === 403) {
        setErrorMessage("Please verify your email before submitting this request.");
        return;
      }

      if (response.ok) {
        setSuccessMessage('Request submitted successfully.');
        sessionStorage.removeItem('signup-request');
        setFormData(prev => ({ ...prev, description: '' }));
      } else {
        setErrorMessage('Submission failed. Please try again.');
      }
    } catch {
      setErrorMessage('Server error occurred.');
    } finally {
      setLoading(false);
    }
  };

  const getCookie = (name: string) => {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop()?.split(';')[0] || '';
    return '';
  };

  return (
    <Box maxW="md" mx="auto" mt={8} px={4}>
      <Heading size="md" mb={4}>Submit Sign-Up Request</Heading>

      {successMessage && (
        <Box bg="green.100" border="1px solid" borderColor="green.300" p={4} mb={4} borderRadius="md" color="green.800">
          {successMessage}
        </Box>
      )}
      {errorMessage && (
        <Box bg="red.100" border="1px solid" borderColor="red.300" p={4} mb={4} borderRadius="md" color="red.800">
          {errorMessage}
        </Box>
      )}

      <form onSubmit={handleSubmit}>
        <Stack spacing={4}>
          <FormControl isRequired>
            <FormLabel>First Name</FormLabel>
            <Input
              name="first_name"
              autoComplete='given-name'
              value={formData.first_name}
              onChange={handleChange}
            />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Last Name</FormLabel>
            <Input
              name="last_name"
              autoComplete='family-name'
              value={formData.last_name}
              onChange={handleChange}
            />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Email</FormLabel>
            <Input
              type="email"
              name="email"
              autoComplete='email'
              value={formData.email}
              readOnly
            />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Description</FormLabel>
            <Textarea
              name="description"
              value={formData.description}
              onChange={handleChange}
            />
          </FormControl>

          <Button
            type="submit"
            colorScheme="purple"
            loading={loading}
            width="full"
          >
            Submit Request
          </Button>
        </Stack>
      </form>
    </Box>
  );
};

export { SignupRequestForm };
