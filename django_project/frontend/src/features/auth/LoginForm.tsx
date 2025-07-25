import React, { useState, useEffect, useRef } from "react";
import { useDispatch, useSelector } from "react-redux";
import { useLocation, useSearchParams } from "react-router-dom";
import {
  Box,
  Button,
  Flex,
  Heading,
  Text,
  Link,
  Input,
  Field,
  Image,
  InputGroup,
  IconButton
} from "@chakra-ui/react";
import { toaster } from "@/components/ui/toaster"
import { FormType } from "./type";
import {
  loginUser,
  registerUser,
  resetPasswordRequest,
  resetPasswordConfirm,
  clearFeedback
} from "./authSlice";
import { RootState, AppDispatch } from "@app/store";
import { loginEvent, socialAuthRedirect } from "@/utils/analytics";
import { useNavigateWithEvent } from "@/hooks/useNavigateWithEvent";
import { useGapContext } from "@/context/GapContext";
import { AiOutlineEye, AiOutlineEyeInvisible } from "react-icons/ai";
import { LocationState } from "@/types";


interface LoginFormProps {
    
}

const LoginForm: React.FC<LoginFormProps> = () => {
    const navigate = useNavigateWithEvent();
    const [showPassword, setShowPassword] = useState(false);
    const [showConfirm, setShowConfirm] = useState(false);
    const [formType, setFormType] = useState<FormType>("signin");
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [confirmPw, setConfirmPw] = useState("");
    const [confirmedEmail, setConfirmedEmail] = useState(false);

    const dispatch = useDispatch<AppDispatch>();
    const { loading, token, message,error } = useSelector((s: RootState) => s.auth);
    const { search, state } = useLocation();

    // Get the page user was trying to access before login
    const locationState = state as LocationState | null;
    const [searchParams] = useSearchParams();
    const from = locationState?.from?.pathname || searchParams.get('next') || '/';

    const params = new URLSearchParams(search);
    const prevType = useRef<FormType>(formType);

    // Get context data
    const { social_auth_providers } = useGapContext();
    const hasSocialLogin = Object.values(social_auth_providers).some(Boolean);

    useEffect(() => {
        if (params.get("uid") && params.get("token")) {
            setFormType("resetPassword");
        } else if (params.get("confirmed") === "true") {
            setConfirmedEmail(true);
            setFormType("signin");
        }
    }, [search]);

    useEffect(() => {
        if (message || error) {
          const id = setTimeout(() => dispatch(clearFeedback()), 5000);
          return () => clearTimeout(id);
        }
      }, [message, error, dispatch]);

    useEffect(() => {
        if (prevType.current !== formType) {
            dispatch(clearFeedback());
            prevType.current = formType;
          }
    }, [formType, dispatch]);

    const handleSubmit = async (e: any) => {
        e.preventDefault();
        const uid = params.get("uid")!;
        const token = params.get("token")!;
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
                if (from) {
                    // If there is a previous page, navigate there
                    navigate(from, null, true, { replace: true });
                } else {
                    // login successful, redirect to home
                    navigate("/", null, true, { replace: true });
                }
            }
            break;
        case "signup":
            dispatch(registerUser(email, password, confirmPw));
            break;
        case "forgotPassword":
            dispatch(resetPasswordRequest(email));
            toaster.create({
                title: "Check your email",
                description: "You will receive a reset link.",
                type: "success"
            });
            break;
        case "resetPassword": {
            // Dispatch the reset-password-confirm thunk
            if (password !== confirmPw) {
                toaster.create({
                    title: "Error",
                    description: "Passwords do not match",
                    type: "error",
                });
                return;
            }
            const result = await dispatch(
                resetPasswordConfirm({ uid, token, password })
            );
        
            if (resetPasswordConfirm.fulfilled.match(result)) {
                toaster.create({
                title: "Password set successfully",
                description: result.payload,
                type: "success",
                });
                // send them back to sign-in
                setFormType("signin");
                navigate("/signin", null, true);
            } else {
                toaster.create({
                title: "Error",
                description: result.payload as string,
                type: "error",
                });
            }
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
                ? "Set Password"
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
                <InputGroup
                    flex="1"
                    endElement={
                        <IconButton
                            aria-label={showPassword ? "Hide password" : "Show password"}
                            variant="ghost"
                            size="xs"
                            onClick={() => setShowPassword((v) => !v)}
                            >
                                {showPassword ? <AiOutlineEyeInvisible /> : <AiOutlineEye />}
                        </IconButton>
                    }
                    >
                    <Input
                        placeholder="Password"
                        type={showPassword ? "text" : "password"}
                        value={password}
                        onChange={(e) => setPassword(e.target.value)}
                        pr="2.5rem"
                    />
                </InputGroup>
            </Field.Root>
            )}

            {/* Confirm Password */}
            {(formType === "signup" || formType === "resetPassword") && (
            <Field.Root required mb={3}>
                <InputGroup
                flex="1"
                endElement={
                    <IconButton
                        aria-label={showConfirm ? "Hide confirm" : "Show confirm"}
                        variant="ghost"
                        size="xs"
                        onClick={() => setShowConfirm((v) => !v)}
                        >
                            {showConfirm ? <AiOutlineEyeInvisible /> : <AiOutlineEye />}
                    </IconButton>
                }
                >
                <Input
                    placeholder="Confirm Password"
                    type={showConfirm ? "text" : "password"}
                    value={confirmPw}
                    onChange={(e) => setConfirmPw(e.target.value)}
                    pr="2.5rem"
                />
                </InputGroup>
            </Field.Root>
            )}

            {/* Forgot */}
            {formType === "signin" && (
            <Flex mb={4} justify="space-between" align="center">
                <Link color="green.600" onClick={() => setFormType("forgotPassword")}>
                Forgot Password?
                </Link>
            </Flex>
            )}

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
                ? "Set Password"
                : "Sign Up"}
            </Button>

            {hasSocialLogin && (
                <Box textAlign="center" mb={4}>
                <Text fontSize="sm" color="gray.500" mb={2}>
                    or continue with
                </Text>

                <Flex justify="center" gap={6}>
                    {social_auth_providers.google && (
                        <Button
                        variant="ghost"
                        p={0}
                        size="xs"
                        aria-label="Login with Google"
                        onClick={() => socialAuthRedirect('google')}
                        >
                            <Image src="/static/images/google_icon.svg" alt="Google" boxSize={6} />
                        </Button>
                    )}

                    {social_auth_providers.github && (
                        <Button
                        variant="ghost"
                        p={0}
                        size="xs"
                        aria-label="Login with GitHub"
                        onClick={() => socialAuthRedirect('github')}
                        >
                            <Image src="/static/images/github_icon.svg" alt="GitHub" boxSize={6} />
                        </Button>
                    )}
                </Flex>
                </Box>
            )}

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