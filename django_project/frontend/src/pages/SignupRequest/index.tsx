// src/components/SignupRequest.tsx
import React, { useEffect, useState } from "react";
import {
  Box,
  Button,
  CloseButton,
  Dialog,
  Field,
  Input,
  Portal,
  Stack,
  Textarea,
  Heading,
  Text,
} from "@chakra-ui/react";
import { AiOutlineMail, AiOutlineUser, AiOutlineHome } from "react-icons/ai";

interface SignupRequestProps {
  isOpen: boolean;
  onClose: () => void;
  user: {
    email: string;
    first_name?: string;
    last_name?: string;
  };
}

export function SignupRequest({ isOpen, onClose, user }: SignupRequestProps) {
  const [formData, setFormData] = useState({
    first_name: user.first_name || '',
    last_name: user.last_name || '',
    email: user.email,
    description: "",
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const uid = params.get("uid");
    if (uid) {
      fetch(`/api/user-uid/${uid}/`)
        .then(res => res.json())
        .then(data => {
          setFormData(prev => ({
            ...prev,
            email: data.email || "",
          }));
        })
        .catch(() => {
          setError("Could not preload user information.");
        });
    }
  }, []);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const getCookie = (name: string): string => {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop()?.split(";")[0] || "";
    return "";
  };

  const handleSubmit = async () => {
    setLoading(true);
    setError("");
    setSuccess("");

    try {
      const res = await fetch("/api/signup-request/", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        credentials: "include",
        body: JSON.stringify(formData),
      });

      if (res.status === 403) {
        setError("Please verify your email before submitting this request.");
      } else if (res.ok) {
        setSuccess("Request submitted successfully.");
        setFormData(prev => ({ ...prev, description: "" }));
        onClose();
      } else {
        setError("Submission failed. Please try again.");
      }
    } catch {
      setError("Server error occurred.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog.Root open={isOpen} onOpenChange={(open: any) => !open && onClose()} modal>
      <Dialog.Backdrop />
      <Portal>
        <Dialog.Positioner>
          <Dialog.Content>
            <Button
                onClick={onClose}
                position="absolute"
                top={2}
                right={2}
                bg={"transparent"}
            >
                <CloseButton />
            </Button>
            <Box p={6} bg="white">
              <Heading size="md" mb={2}>Join Our Waitlist</Heading>
              <Text fontSize="sm" mb={4}>Please share your information and we will be in touch within 1 week.</Text>

              <Stack gap={4}>
                <Field.Root required>
                  <Field.Label>First Name</Field.Label>
                  <Input
                    name="first_name"
                    value={formData.first_name}
                    onChange={handleChange}
                    placeholder="First Name"
                  />
                </Field.Root>

                <Field.Root required>
                  <Field.Label>Last Name</Field.Label>
                  <Input
                    name="last_name"
                    value={formData.last_name}
                    onChange={handleChange}
                    placeholder="Last Name"
                  />
                </Field.Root>

                <Field.Root required>
                  <Field.Label>Email</Field.Label>
                  <Input
                    name="email"
                    value={formData.email}
                    readOnly
                    type="email"
                    placeholder="email@example.com"
                  />
                </Field.Root>

                <Field.Root required>
                  <Field.Label>Description</Field.Label>
                  <Textarea
                    name="description"
                    value={formData.description}
                    onChange={handleChange}
                    placeholder="Tell us what you're looking for..."
                  />
                </Field.Root>

                {error && (
                  <Text fontSize="sm" color="red.600">{error}</Text>
                )}

                <Button
                  w="full"
                  bg={"green.400"}
                  color="white"
                  _hover={{ bg: "green.500" }}
                  rounded="full"
                  fontWeight="bold"
                  mb={4}
                  loading={loading}
                  onClick={handleSubmit}
                >
                  Submit
                </Button>
              </Stack>
            </Box>
          </Dialog.Content>
        </Dialog.Positioner>
      </Portal>
    </Dialog.Root>
  );
}
