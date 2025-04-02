import React, { useState } from 'react';
import {
  Box,
  Button,
  Input,
  Textarea,
  Heading
} from '@chakra-ui/react';
import { FormControl, FormLabel } from '@chakra-ui/form-control';
import { useToast } from '@chakra-ui/toast';
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
  const toast = useToast();

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);

    try {
      const response = await fetch('/api/signup-request/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': getCookie('csrftoken'),
        },
        body: JSON.stringify(formData),
      });

      if (response.ok) {
        toast({ title: 'Request submitted', status: 'success', isClosable: true });
        setFormData(prev => ({ ...prev, description: '' }));
      } else {
        toast({ title: 'Submission failed', status: 'error', isClosable: true });
      }
    } catch {
      toast({ title: 'Server error', status: 'error', isClosable: true });
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
      <form onSubmit={handleSubmit}>
        <Stack spacing={4}>
          <FormControl isRequired>
            <FormLabel>First Name</FormLabel>
            <Input
              name="first_name"
              value={formData.first_name}
              onChange={handleChange}
            />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Last Name</FormLabel>
            <Input
              name="last_name"
              value={formData.last_name}
              onChange={handleChange}
            />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Email</FormLabel>
            <Input
              type="email"
              name="email"
              value={formData.email}
              onChange={handleChange}
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
