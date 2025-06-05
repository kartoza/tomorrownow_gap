// theme.ts
import { defineConfig, createSystem, defaultConfig, Button } from '@chakra-ui/react'

// Define your custom theme configuration
const config = defineConfig({
  theme: {
    tokens: {
      colors: {
        // Your brand colors
        brand: {
          50: { value: '#E8FDF4' },   // Very light green
          100: { value: '#B7CCB7' },  // Light green - design
          200: { value: '#9EF5CC' },  // Lighter green
          300: { value: '#79F1B8' },  // Light glowing green
          400: { value: '#54EDA4' },  // Medium glowing green
          500: { value: '#3AF1A3' },  // Your primary glowing green - design
          600: { value: '#38453C' },  // Darker green - design
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
        heading: { value: `Mulish, sans-serif` },
        body: { value: `Mulish, sans-serif` },
      },
      fontSizes: {
        xs: { value: '0.75rem' },     // 12px
        sm: { value: '0.875rem' },    // 14px
        base: { value: '1rem' },      // 16px
        '2base': { value: '2rem' },      // 32px
        md: { value: '1.125rem' },    // 18px - modified
        lg: { value: '1.25rem' },     // 20px
        '2lg': { value: '2.5rem' },  // 40px
        xl: { value: '1.5rem' },      // 24px
        '2xl': { value: '1.875rem' }, // 30px
        '3xl': { value: '2.25rem' },  // 36px
        '4xl': { value: '3rem' },     // 48px
        '5xl': { value: '4rem' },     // 64px
        '6xl': { value: '4.5rem' },   // 72px
        '7xl': { value: '5rem' },     // new size
      },
      fontWeights: {
        normal: {value: 400},
        medium: {value: 500},
        semibold: {value: 600},
        bold: {value: 700},
        extrabold: {value: 800},
        black: {value: 900},
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
    recipes: {
      button: {
        variants: {
          visual: {
            
            outline: { borderWidth: "1px", borderColor: "brand.500" },
          },
          size: {
            sm: { padding: "4", fontSize: "12px" },
            md: { padding: "16px", fontSize: "16px", minW: "200px", minH: "54px", gap: "10px", } 
          },
          variant: {
            solid: { bg: "brand.500", color: "brand.600", borderRadius: "25px", p: "10px" },
            landingPage: { bg: "brand.500", color: "brand.600", borderRadius: "50px", fontWeight: "bold", _hover: { bg: "brand.600", color: "white" }, _active: { bg: "brand.700", color: "white" } }
          }
        }
      }
    }
  },
  globalCss: {
    body: {
      bg: 'white',
      color: '{text.primary}',
      fontSize: 'base',
      fontFamily: '{fonts.body}',
    },
  }
})

// Note:
// Updating theme, may require to execute `npx @chakra-ui/cli typegen ./src/theme.ts`
// to generate the types for the new theme configuration.

const system = createSystem(defaultConfig, config)


export default system
