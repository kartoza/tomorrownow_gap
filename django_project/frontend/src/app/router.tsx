import { createBrowserRouter } from 'react-router-dom'
import { 
  Box, 
  Container, 
  VStack, 
  Heading, 
  Text, 
  Button,
  Icon
} from '@chakra-ui/react'
import { MainLayout } from '@/layouts/MainLayout'
import { AuthLayout } from '@/layouts/AuthLayout'
import LandingPage from '@/pages/LandingPage'
import LoginPage from '@/pages/LoginPage'
import SignupPage from '@/pages/SignupPage'
import DcasCsvList from '@/pages/DcasCsvList'
import DataFormsPage from '@/pages/DataFormsPage'


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


export const router = createBrowserRouter([
  {
    path: "/",
    element: <MainLayout />,
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <LandingPage />,
      }
    ],
  },
  {
    path: "/signin",
    element: <AuthLayout />,
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <LoginPage />,
      }
    ],
  },
  {
    path: "/signup",
    element: <AuthLayout />,
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <SignupPage />,
      }
    ],
  },
  {
    path: "/dcas-csv",
    element: <MainLayout />,
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <DcasCsvList />,
      }
    ]
  },
  {
    path: "/data-forms",
    element: <MainLayout />,
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <DataFormsPage />,
      }
    ]
  },
])