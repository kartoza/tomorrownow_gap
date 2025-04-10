import React, { useState, useEffect } from 'react';
import {
  Box,
  Button,
  Input,
  Heading,
} from '@chakra-ui/react';
import { FormControl, FormLabel } from '@chakra-ui/form-control';
import { Stack } from '@chakra-ui/layout';
import { isPasswordStrong } from '../../utils/validation';

const SignupAccountForm = () => {
  const [formData, setFormData] = useState({
    email: '',
    password: '',
    confirm_password: '',
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
    setSuccessMessage('');
    setErrorMessage('');

    const passwordError = isPasswordStrong(formData.password);
    if (passwordError) {
      setErrorMessage(`Weak password: ${passwordError}`);
      setLoading(false);
      return;
    }

    if (formData.password.trim() !== formData.confirm_password.trim()) {
      setErrorMessage('Passwords do not match');
      setLoading(false);
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(formData.email)) {
      setErrorMessage('Invalid email address');
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

      const text = await response.text();
      let data;
      try {
        data = JSON.parse(text);
      } catch (err) {
        console.log("Response parsing error", err);
      }

      if (response.ok) {
        window.location.href = "check_email/";
        setFormData({ email: '', password: '', confirm_password: '' });
      } else {
        if (data?.email?.[0] === "This email is already registered.") {
          setErrorMessage("Email already in use");
        } else if (typeof data.detail === 'string') {
          setErrorMessage(`Signup failed: ${data.detail}`);
        } else {
          setErrorMessage("Signup failed. Please check your input.");
        }
      }
    } catch (err) {
      setErrorMessage('Server error. Something went wrong.');
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
        Create Your Account
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
            <FormLabel>Email</FormLabel>
            <Input type="email" name="email" value={formData.email} onChange={handleChange} />
            {formData.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.email) && (
              <Box mt={2} color="red.500" fontSize="sm">
                Please enter a valid email address
              </Box>
            )}
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Password</FormLabel>
            <Input type="password" name="password" value={formData.password} onChange={handleChange} />
            {formData.password && (
              <Box mt={2} color="red.500" fontSize="sm">
                {isPasswordStrong(formData.password)}
              </Box>
            )}
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Confirm Password</FormLabel>
            <Input type="password" name="confirm_password" value={formData.confirm_password} onChange={handleChange}/>
            {formData.confirm_password && formData.password !== formData.confirm_password && (
              <Box mt={2} color="red.500" fontSize="sm">
                Passwords do not match
              </Box>
            )}
            {formData.confirm_password && formData.password === formData.confirm_password && (
              <Box mt={2} color="green.500" fontSize="sm">
                Passwords match
              </Box>
            )}
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
