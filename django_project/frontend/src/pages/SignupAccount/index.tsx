import React, { useState } from 'react';
import {
  Box,
  Button,
  Input,
} from '@chakra-ui/react';
import { FormControl, FormLabel } from '@chakra-ui/form-control';
import { useToast } from '@chakra-ui/toast';
import { Stack } from '@chakra-ui/layout';

const SignupAccountForm = () => {
  const toast = useToast();
  const [formData, setFormData] = useState({
    first_name: '',
    last_name: '',
    email: '',
    password: '',
    confirm_password: '',
  });

  const [loading, setLoading] = useState(false);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);

    // Confirm passwords match before submitting
    if (formData.password !== formData.confirm_password) {
        toast({
        title: 'Passwords do not match',
        status: 'error',
        isClosable: true,
        });
        setLoading(false);
        return;
    }

    try {
      const response = await fetch('/api/auth/register/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': getCookie('csrftoken'),
        },
        body: JSON.stringify(formData),
      });

      const data = await response.json();

      if (response.ok) {
        toast({
          title: 'Check your email.',
          description: 'We’ve sent a verification link.',
          status: 'success',
          isClosable: true,
        });
        setFormData({ first_name: '', last_name: '', email: '', password: '', confirm_password: '' });
      } else {
        toast({
          title: 'Signup failed',
          description: data.detail || 'Please try again.',
          status: 'error',
          isClosable: true,
        });
      }
    } catch (err) {
      toast({
        title: 'Server error',
        description: 'Something went wrong.',
        status: 'error',
        isClosable: true,
      });
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

          <FormControl isRequired>
            <FormLabel>Password</FormLabel>
            <Input type="password" name="password" value={formData.password} onChange={handleChange} />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Confirm Password</FormLabel>
            <Input type="password" name="confirm_password" value={formData.confirm_password} onChange={handleChange}/>
            </FormControl>

          <Button type="submit" colorScheme="purple" loading={loading}>
            Create Account
          </Button>
        </Stack>
      </form>
    </Box>
  );
};

export { SignupAccountForm };
