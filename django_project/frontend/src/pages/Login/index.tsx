import React, { useState } from 'react';
import {
  Box,
  Button,
  Input,
  Heading,
} from '@chakra-ui/react';
import { FormControl, FormLabel } from '@chakra-ui/form-control';
import { Stack } from '@chakra-ui/layout';
import toast from 'react-hot-toast';

const LoginAccountForm = () => {
  const [formData, setFormData] = useState({
    username: '',
    password: '',
  });

  const [loading, setLoading] = useState(false);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);

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
        toast.success('Logged in successfully!');
        const data = await response.json();
        window.location.href = data.redirect_url;
      } else {
        toast.error('Invalid username or password');
      }
    } catch (err) {
      toast.error('Server error. Please try again.');
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

          <Button type="submit" colorScheme="purple" loading={loading} width="full">
            Log In
          </Button>
        </Stack>
      </form>
    </Box>
  );
};

export { LoginAccountForm };
