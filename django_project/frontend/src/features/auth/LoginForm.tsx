import React, { useState, useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import { useLocation } from "react-router-dom";
import {
  Box,
  Button,
  Flex,
  Heading,
  Text,
  Link,
  Input,
  Field
} from "@chakra-ui/react";
import { toaster } from "@/components/ui/toaster"
import { FormType } from "./type";
import {
  loginUser,
  registerUser,
  resetPasswordRequest,
  resetPasswordConfirm
} from "./authSlice";
import { RootState, AppDispatch } from "@app/store";
import { loginEvent } from "@/utils/analytics";
import { useNavigateWithEvent } from "@/hooks/useNavigateWithEvent";


interface LoginFormProps {
    
}

const LoginForm: React.FC<LoginFormProps> = () => {
    const navigate = useNavigateWithEvent();
    const [formType, setFormType] = useState<FormType>("signin");
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [confirmPw, setConfirmPw] = useState("");
    const [confirmedEmail, setConfirmedEmail] = useState(false);

    const dispatch = useDispatch<AppDispatch>();
    const { loading, token, message,error } = useSelector((s: RootState) => s.auth);
    const { search } = useLocation();

    useEffect(() => {
        const params = new URLSearchParams(search);
        if (params.get("uid") && params.get("token")) {
            setFormType("resetPassword");
        } else if (params.get("confirmed") === "true") {
            setConfirmedEmail(true);
            setFormType("signin");
        }
    }, [search]);

    const handleSubmit = async (e: any) => {
        e.preventDefault();
        switch (formType) {
        case "signin":
            loginEvent();
            const resultAction = await dispatch(loginUser({email, password}));
            if (loginUser.fulfilled.match(resultAction)) {
                toaster.create({
                    title: "Welcome Back!",
                    description: "You have successfully logged in.",
                    type: "success"
                });
                // login successful, redirect to home
                navigate("/", null, true);
            }
            break;
        case "signup":
            dispatch(registerUser(email, password, confirmPw));
            break;
        case "forgotPassword":
            dispatch(resetPasswordRequest(email));
            break;
        case "resetPassword": {
            const params = new URLSearchParams(search);
            dispatch(
                resetPasswordConfirm(
                    params.get("uid") as string,
                    params.get("token") as string,
                    password
                )
            );
            break;
        }
        }
    };

    return (
        <Box p={6} bg={"white"} borderRadius="md" maxW={{ base: "full", md: "75%" }} boxShadow="md" mx="auto" color={"text.primary"}
            as="form" onSubmit={handleSubmit}>
            <Heading mb={2} fontSize={"2xl"}>
            {formType === "signin"
                ? "Welcome Back!"
                : formType === "forgotPassword"
                ? "Forgot Password"
                : formType === "resetPassword"
                ? "Reset Password"
                : "Sign Up"}
            </Heading>
            {confirmedEmail && (
                <Text color="green.600">Your email has been confirmed successfully. We will be in touch within 1 week</Text>
            )}
            <Text mb={4}>
            {formType === "signin"
                ? "Please sign into your profile."
                : formType === "forgotPassword"
                ? "Enter your email to receive a reset link."
                : formType === "resetPassword"
                ? "Please set your new password."
                : "Create a new account."}
            </Text>
            {/* Message */}
            {message && (
            <Box
                color="green.600"
                bg="green.50"
                border="1px solid"
                borderColor="green.300"
                p={3}
                mb={4}
                borderRadius="md"
                fontSize="sm"
            >
                {message}
            </Box>
            )}

            {error && (
            <Box
                color="red.600"
                bg="red.50"
                border="1px solid"
                borderColor="red.300"
                p={3}
                mb={4}
                borderRadius="md"
                fontSize="sm"
            >
                {error}
            </Box>
            )}
            {/* Email */}
            {formType !== "resetPassword" && (
            <Field.Root required mb={3} invalid={!!error}>
                <Input
                    placeholder="you@example.com"
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                />
                <Field.ErrorText>{error}</Field.ErrorText>
            </Field.Root>
            )}

            {/* Password */}
            {formType !== "forgotPassword" && (
            <Field.Root required mb={3}>
                <Input
                    placeholder="Password"
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                />
            </Field.Root>
            )}

            {/* Confirm Password */}
            {(formType === "signup" || formType === "resetPassword") && (
            <Field.Root required mb={3}>
                <Input
                    placeholder="Confirm Password"
                    type="password"
                    value={confirmPw}
                    onChange={(e) => setConfirmPw(e.target.value)}
                />
            </Field.Root>
            )}

            {/* Forgot */}
            {/* {formType === "signin" && (
            <Flex mb={4} justify="space-between" align="center">
                <Link color="green.600" onClick={() => setFormType("forgotPassword")}>
                Forgot Password?
                </Link>
            </Flex>
            )} */}

            <Button
                w="full"
                visual={"solid"}
                size={"md"}
                type="submit"
                fontWeight="bold"
                mb={4}
                loading={loading}
            >
            {formType === "signin"
                ? "Sign In"
                : formType === "forgotPassword"
                ? "Send Email"
                : formType === "resetPassword"
                ? "Reset Password"
                : "Sign Up"}
            </Button>

            {/* <Box textAlign="center" mb={4}>
            <Text fontSize="sm" color="gray.500" mb={2}>
                or continue with
            </Text>
            <Flex justify="center" gap={6}>
                <Link href="/accounts/google/login/" aria-label="Login with Google">
                <Image src="/static/images/google_icon.svg" alt="Google login" boxSize={6} />
                </Link>
                <Link href="/accounts/github/login/" aria-label="Login with GitHub">
                <Image src="/static/images/github_icon.svg" alt="GitHub login" boxSize={6} />
                </Link>
                <Link href="/accounts/apple/login/" aria-label="Login with Apple">
                <Image src="/static/images/apple_icon.svg" alt="Apple login" boxSize={6} />
                </Link>
            </Flex>
            </Box> */}

            <Flex justify="center">
            {formType === "signin" ? (
                <Text fontSize="sm">
                Don’t have an account?{" "}
                <Link color="green.600" onClick={() => navigate("/signup", 'login_form_sign_up')}>
                    Sign Up
                </Link>
                </Text>
            ) : (
                <Link color="green.600" onClick={() => setFormType("signin")}>
                Back to Sign In
                </Link>
            )}
            </Flex>
        </Box>
    );
};

export default LoginForm;