import React from "react";
import { 
  Box, 
  Container, 
  VStack, 
  Heading, 
  Text, 
  Button,
} from '@chakra-ui/react'


const ErrorPage = () => (
  <Box minH="100vh" display="flex" alignItems="center" justifyContent="center" bg="gray.50">
    <Container maxW="md" textAlign="center">
      <VStack gap={6}>
        <Box fontSize="6xl" color="red.500">
          ⚠️
        </Box>
        <Heading as="h1" size="xl" color="gray.900">
          Oops! Something went wrong
        </Heading>
        <Text color="gray.600" fontSize="lg">
          We couldn't find the page you're looking for, or something unexpected happened.
        </Text>
        <VStack gap={3}>
          <Button
            colorScheme="blue"
            size="lg"
            onClick={() => window.location.href = '/'}
          >
            Go Back Home
          </Button>
          <Button
            variant="outline"
            colorScheme="gray"
            onClick={() => window.location.reload()}
          >
            Refresh Page
          </Button>
        </VStack>
        <Text fontSize="sm" color="gray.500">
          If this problem persists, please contact our support team.
        </Text>
      </VStack>
    </Container>
  </Box>
)

export default ErrorPage;