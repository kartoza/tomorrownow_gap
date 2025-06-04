// theme.ts
import { defineConfig, createSystem, defaultConfig } from '@chakra-ui/react'

// Define your custom theme configuration
const config = defineConfig({
  theme: {
    tokens: {
      colors: {
        // Your brand colors
        brand: {
          50: { value: '#E8FDF4' },   // Very light green
          100: { value: '#C3F9E0' },  // Light green
          200: { value: '#9EF5CC' },  // Lighter green
          300: { value: '#79F1B8' },  // Light glowing green
          400: { value: '#54EDA4' },  // Medium glowing green
          500: { value: '#3AF1A3' },  // Your primary glowing green
          600: { value: '#2EC182' },  // Darker green
          700: { value: '#229161' },  // Much darker green
          800: { value: '#166140' },  // Very dark green
          900: { value: '#0A311F' },  // Darkest green
        },
        // Your text colors
        text: {
          primary: { value: '#38453C' },    // Your dark green text color
          secondary: { value: '#5A6B5E' },  // Lighter variation
          muted: { value: '#7A8B7E' },      // Even lighter for secondary text
        },
        // Standard grays
        gray: {
          50: { value: '#F7FAFC' },
          100: { value: '#EDF2F7' },
          200: { value: '#E2E8F0' },
          300: { value: '#CBD5E0' },
          400: { value: '#A0AEC0' },
          500: { value: '#718096' },
          600: { value: '#4A5568' },
          700: { value: '#2D3748' },
          800: { value: '#1A202C' },
          900: { value: '#171923' },
        }
      },
      fonts: {
        heading: { value: `'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif` },
        body: { value: `'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif` },
      },
    },
    semanticTokens: {
      colors: {
        // Semantic color mappings
        'color-palette': {
          50: { value: '{brand.50}' },
          100: { value: '{brand.100}' },
          200: { value: '{brand.200}' },
          300: { value: '{brand.300}' },
          400: { value: '{brand.400}' },
          500: { value: '{brand.500}' },
          600: { value: '{brand.600}' },
          700: { value: '{brand.700}' },
          800: { value: '{brand.800}' },
          900: { value: '{brand.900}' },
        },
        // Default text colors
        fg: { value: '{text.primary}' },
        'fg.muted': { value: '{text.secondary}' },
        'fg.subtle': { value: '{text.muted}' },
      }
    },
    // globalCss: {
    //   body: {
    //     bg: 'white',
    //     color: 'text.primary',
    //   },
    //   '*::placeholder': {
    //     color: 'text.muted',
    //   },
    // },
  },
  globalCss: {
    body: {
      bg: 'white',
      color: '{text.primary}',
    },
  },
})

// Create the system
// const system = createSystem(config, {
//   // Component recipes/styles
//   recipes: {
//     button: {
//       base: {
//         fontWeight: 'medium',
//         borderRadius: 'md',
//         _focusVisible: {
//           outline: '2px solid',
//           outlineColor: 'brand.500',
//           outlineOffset: '2px',
//         },
//       },
//       variants: {
//         variant: {
//           solid: {
//             bg: 'brand.500',
//             color: 'white',
//             _hover: {
//               bg: 'brand.600',
//               transform: 'translateY(-2px)',
//               boxShadow: '0 4px 12px rgba(58, 241, 163, 0.4)',
//             },
//             _active: {
//               bg: 'brand.700',
//               transform: 'translateY(0)',
//             },
//           },
//           outline: {
//             borderWidth: '1px',
//             borderColor: 'brand.500',
//             color: 'brand.500',
//             _hover: {
//               bg: 'brand.50',
//               borderColor: 'brand.600',
//             },
//           },
//           ghost: {
//             color: 'brand.500',
//             _hover: {
//               bg: 'brand.50',
//             },
//           },
//         },
//         size: {
//           sm: {
//             h: '8',
//             minW: '8',
//             fontSize: 'sm',
//             px: '3',
//           },
//           md: {
//             h: '10',
//             minW: '10',
//             fontSize: 'sm',
//             px: '4',
//           },
//           lg: {
//             h: '12',
//             minW: '12',
//             fontSize: 'md',
//             px: '6',
//           },
//         },
//       },
//       defaultVariants: {
//         variant: 'solid',
//         size: 'md',
//       },
//     },
//     heading: {
//       base: {
//         color: 'text.primary',
//         fontWeight: 'bold',
//       },
//     },
//     text: {
//       base: {
//         color: 'text.primary',
//       },
//     },
//     link: {
//       base: {
//         color: 'brand.500',
//         _hover: {
//           color: 'brand.600',
//           textDecoration: 'none',
//         },
//       },
//     },
//   },
// })
const system = createSystem(defaultConfig, config)


export default system

// Type extensions for better TypeScript support
// declare module '@chakra-ui/react' {
//   interface Token {
//     colors: {
//       brand: {
//         50: string
//         100: string
//         200: string
//         300: string
//         400: string
//         500: string
//         600: string
//         700: string
//         800: string
//         900: string
//       }
//       text: {
//         primary: string
//         secondary: string
//         muted: string
//       }
//     }
//   }
// }