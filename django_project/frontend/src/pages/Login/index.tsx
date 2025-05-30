// src/components/SignIn.tsx
import React, { useEffect, useState } from "react";
import {
  Dialog,
  Portal,
  Button,
  CloseButton,
  Field,
  Input,
  Flex,
  Heading,
  Text,
  Box,
  Link,
  VStack,
  HStack,
  Image,
} from "@chakra-ui/react";
import { AiOutlineMail, AiOutlineLock } from "react-icons/ai";
import { useDispatch, useSelector } from "react-redux";
import {
  loginUser,
  registerUser,
  resetPasswordRequest,
  resetPasswordConfirm,
} from "../../store/authSlice";
import { RootState, AppDispatch } from "../../store";
import { useLocation } from "react-router-dom";

interface SignInProps {
  isOpen: boolean;
  onClose: () => void;
}

type FormType = "signin" | "signup" | "forgotPassword" | "resetPassword";

export default function SignIn({ isOpen, onClose }: SignInProps) {
  const [formType, setFormType] = useState<FormType>("signin");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPw, setConfirmPw] = useState("");


  const dispatch = useDispatch<AppDispatch>();
  const { loading, token, error } = useSelector((s: RootState) => s.auth);
  const { search } = useLocation();

  useEffect(() => {
    const params = new URLSearchParams(search);
    if (params.get("uid") && params.get("token")) {
      setFormType("resetPassword");
    }
  }, [search]);

  useEffect(() => {
    if (token) {
      onClose();
      setFormType("signin");

      // Redirect to Swagger docs
      window.location.href = "/api/v1/docs/";
    }
  }, [token, onClose]);

  const handleSubmit = () => {
    switch (formType) {
      case "signin":
        dispatch(loginUser(email, password));
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
    <Dialog.Root
      open={isOpen}
      onOpenChange={(open: any) => {
        if (!open) onClose();
      }}
      modal
    >
      <Portal>
        <Dialog.Backdrop />
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

            <Box p={6}>
              <Heading size="lg" mb={2}>
                {formType === "signin"
                  ? "Welcome Back!"
                  : formType === "forgotPassword"
                  ? "Forgot Password"
                  : formType === "resetPassword"
                  ? "Reset Password"
                  : "Sign Up"}
              </Heading>
              <Text mb={4} color="gray.600">
                {formType === "signin"
                  ? "Please sign into your profile."
                  : formType === "forgotPassword"
                  ? "Enter your email to receive a reset link."
                  : formType === "resetPassword"
                  ? "Please set your new password."
                  : "Create a new account."}
              </Text>

              {/* Email */}
              {formType !== "resetPassword" && (
                <Field.Root required mb={3} invalid={!!error}>
                  <Field.Label>Email</Field.Label>
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
                  <Field.Label>Password</Field.Label>
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
                  <Field.Label>Confirm Password</Field.Label>
                    <Input
                      placeholder="Confirm Password"
                      type="password"
                      value={confirmPw}
                      onChange={(e) => setConfirmPw(e.target.value)}
                    />
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
                bg={"green.400"}
                color="white"
                _hover={{ bg: "green.500" }}
                rounded="full"
                fontWeight="bold"
                mb={4}
                loading={loading}
                onClick={handleSubmit}
              >
                {formType === "signin"
                  ? "Sign In"
                  : formType === "forgotPassword"
                  ? "Send Email"
                  : formType === "resetPassword"
                  ? "Reset Password"
                  : "Sign Up"}
              </Button>

              <Box textAlign="center" mb={4}>
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
              </Box>

              <Flex justify="center">
                {formType === "signin" ? (
                  <Text fontSize="sm">
                    Don’t have an account?{" "}
                    <Link color="green.600" onClick={() => setFormType("signup")}>
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
          </Dialog.Content>
        </Dialog.Positioner>
      </Portal>
    </Dialog.Root>
  );
}
