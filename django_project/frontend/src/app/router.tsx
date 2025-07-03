import { createBrowserRouter } from 'react-router-dom'
import { MainLayout } from '@/layouts/MainLayout'
import { AuthLayout } from '@/layouts/AuthLayout'
import { MapLayout } from '@/layouts/MapLayout'
import LandingPage from '@/pages/LandingPage'
import LoginPage from '@/pages/LoginPage'
import SignupPage from '@/pages/SignupPage'
import DcasCsvList from '@/pages/DcasCsvList'
import ErrorPage from '@/pages/ErrorPage'
import ProtectedRoute from '@/components/ProtectedRoute'
import {SearchForm} from '@/pages/DataBrowserPage'
import ApiKeys from '@/pages/ApiKeys'
import DataFormsPage from '@/pages/DataFormsPage'


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
    element: (
      <ProtectedRoute>
        <MainLayout />
      </ProtectedRoute>
    ),
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <DcasCsvList />,
      }
    ]
  },
  {
    path: "/data-browser",
    element: (
      <ProtectedRoute>
        <MapLayout />
      </ProtectedRoute>
    ),
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <SearchForm />,
      }
    ]
  },
  {
    path: "/api-keys",
    element: (
      <ProtectedRoute>
        <MainLayout />
      </ProtectedRoute>
    ),
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <ApiKeys />,
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