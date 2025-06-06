import React, { useState } from "react";
import {
  Box,
  Button,
  Field,
  Heading,
  Input,
  Stack,
  Text,
  Textarea,
  Flex,
  Link
} from "@chakra-ui/react";
import { useNavigate } from "react-router-dom";


interface WaitListFormProps {
  user?: {
    email: string;
    first_name?: string;
    last_name?: string;
  };
}


const WaitListForm: React.FC<WaitListFormProps> = ({user}) => {
    const navigate = useNavigate();
    const [formData, setFormData] = useState({
        first_name: user?.first_name || '',
        last_name: user?.last_name || '',
        email: user?.email,
        organisation: "",
        description: "",
    });
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");
    const [success, setSuccess] = useState("");

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
          } else {
            setError("Submission failed. Please try again.");
          }
        } catch {
          setError("Server error occurred.");
        } finally {
          setLoading(false);
        }
    };

    // if (submitted) {
    //     return <div>Thank you for joining the wait list!</div>;
    // }

    return (
        <Box p={6} bg={"white"} borderRadius="md" maxW={{ base: "full", md: "75%" }} boxShadow="md" mx="auto"  color={"text.primary"}>
            <Heading mb={2} fontSize={"2xl"}>Join Our Waitlist</Heading>
            <Text mb={4}>Please share your information and we will be in touch within 1 week.</Text>

            <Stack gap={4}>
                <Field.Root required>
                    <Input
                    name="first_name"
                    value={formData.first_name}
                    onChange={handleChange}
                    placeholder="First Name"
                    />
                </Field.Root>

                <Field.Root required>
                    <Input
                    name="last_name"
                    value={formData.last_name}
                    onChange={handleChange}
                    placeholder="Last Name"
                    />
                </Field.Root>

                <Field.Root required>
                    <Input
                    name="email"
                    value={formData.email}
                    type="email"
                    placeholder="email@example.com"
                    />
                </Field.Root>

                <Field.Root required>
                    <Input
                    name="organisation"
                    value={formData.organisation}
                    onChange={handleChange}
                    placeholder="Enter your organisation"
                    />
                </Field.Root>

                <Field.Root required>
                    <Textarea
                        name="description"
                        value={formData.description}
                        onChange={handleChange}
                        placeholder="Tell us what you're looking for..."
                        rows={4}
                    />
                </Field.Root>

                {error && (
                    <Text fontSize="sm" color="red.600">{error}</Text>
                )}

                <Button
                    w="full"
                    visual={"solid"}
                    size={"md"}
                    fontWeight="bold"
                    mb={4}
                    loading={loading}
                    onClick={handleSubmit}
                >
                    Submit
                </Button>

                <Flex justify="center">
                    <Text fontSize="sm">
                        Already have an account?{" "}
                        <Link color="green.600" onClick={() => navigate("/signin")}>
                            Log In
                        </Link>
                    </Text>
                </Flex>
            </Stack>
        </Box>
    );
};

export default WaitListForm;