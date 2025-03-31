// pages/Signup/index.tsx

import React from 'react';
import {
  Box, Button, Input,
  Textarea, Heading
} from '@chakra-ui/react';
import { FormControl, FormLabel } from '@chakra-ui/form-control';
import { useToast } from '@chakra-ui/toast';
import { Stack } from '@chakra-ui/layout';

const SignupForm = () => {
  const [formData, setFormData] = React.useState({
    first_name: '',
    last_name: '',
    email: '',
    description: '',
  });

  const toast = useToast();

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    try {
      const response = await fetch('/api/signup/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': getCookie('csrftoken'),
        },
        body: JSON.stringify(formData),
      });

      if (response.ok) {
        toast({ title: 'Sign-up submitted', status: 'success' });
        setFormData({
          first_name: '',
          last_name: '',
          email: '',
          description: '',
        });
      } else {
        toast({ title: 'Error submitting form', status: 'error' });
      }
    } catch (err) {
      toast({ title: 'Unexpected error', status: 'error' });
    }
  };

  const getCookie = (name: string) => {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop()?.split(';').shift() || '';
    return '';
  };

  return (
    <Box>
      <Heading size="md" mb={4}>Request Access</Heading>
      <form onSubmit={handleSubmit}>
        <Stack spacing={4}>
          <FormControl isRequired>
            <FormLabel>First Name</FormLabel>
            <Input name="first_name" value={formData.first_name} onChange={handleChange} />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Last Name</FormLabel>
            <Input name="last_name" value={formData.last_name} onChange={handleChange} />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Email</FormLabel>
            <Input type="email" name="email" value={formData.email} onChange={handleChange} />
          </FormControl>

          <FormControl>
            <FormLabel>Description</FormLabel>
            <Textarea name="description" value={formData.description} onChange={handleChange} />
          </FormControl>

          <Button type="submit" colorScheme="purple" width="full">
            Submit Request
          </Button>
        </Stack>
      </form>
    </Box>
  );
};

export { SignupForm };
