import React, { useState } from 'react';
import {
  Box,
  Button,
  Input,
  Heading
} from '@chakra-ui/react';
import { FormControl, FormLabel } from '@chakra-ui/form-control';
import { Stack } from '@chakra-ui/layout';

const LoginAccountForm = () => {
  const [formData, setFormData] = useState({
    username: '',
    password: '',
  });

  const [loading, setLoading] = useState(false);
  const [successMessage, setSuccessMessage] = useState('');
  const [errorMessage, setErrorMessage] = useState('');

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setErrorMessage('');
    setSuccessMessage('');

    try {
      const response = await fetch('/api/auth/login/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': getCookie('csrftoken'),
        },
        body: JSON.stringify({
          username: formData.username,
          password: formData.password,
        }),
      });

      if (response.ok) {
        const data = await response.json();
        setSuccessMessage('Login successful! Redirecting...');
        setTimeout(() => {
          window.location.href = data.redirect_url;
        }, 1500);
      } else {
        setErrorMessage('Invalid username or password.');
      }
    } catch (err) {
      setErrorMessage('Server error. Please try again.');
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
    <Box maxW="sm" mx="auto" mt={8} px={4}>
      <Heading as="h1" size="lg" textAlign="center" mb={6}>
        Log In
      </Heading>

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
            <FormLabel>Username</FormLabel>
            <Input
              type="text"
              name="username"
              value={formData.username}
              onChange={handleChange}
            />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Password</FormLabel>
            <Input
              type="password"
              name="password"
              value={formData.password}
              onChange={handleChange}
            />
          </FormControl>

          <Button
            type="submit"
            colorScheme="purple"
            loading={loading}
            width="full"
          >
            Log In
          </Button>
        </Stack>
      </form>
    </Box>
  );
};

export { LoginAccountForm };
