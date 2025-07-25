import { createBrowserRouter } from 'react-router-dom'
import { FullMapLayout } from '@/layouts/FullMapLayout'
import SimpleNavBar from '@/components/SimpleNavBar'
import ErrorPage from '@/pages/ErrorPage'


export const router = createBrowserRouter([
  {
    path: "/api/v1/docs",
    element: <FullMapLayout />,
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <div></div>,
      }
    ],
  }
])

export const navRouter = createBrowserRouter([
  {
    path: "/api/v1/docs",
    element: <SimpleNavBar />,
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <div></div>,
      }
    ],
  }
])