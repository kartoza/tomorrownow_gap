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
          secondary: { value: '#3AF1A3' },  // Lighter variation
          muted: { value: '#B7CCB7' },      // Even lighter for secondary text
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
        'base': { value: '1rem' }, // 16px
        '2base': { value: '2rem' },      // 32px
        'giant': { value: '4rem' }, // 64px
        'subGiant': { value: '2.5rem' }, // 40px
      },
      radii: {
        '2lg': { value: '0.625rem' }, // 10px
        'buttonSm': {value: '1.5625rem'}, // 25px
        'buttonLg': {value: '3.125rem'}, // 50px
      },
      sizes: {
        'pLg': { value: '1.5625rem'}, // 25px
        'minWLg': {value: '12.5rem' }, // 200px
        'minHlg': {value: '3.375rem' }, // 54px
        'minWSm': {value: '5.625rem' }, // 90px
      },
      letterSpacings: {
        'default': { value: '0.009375em' }, // Default letter spacing 0.15px,
        'empty': { value: '0.0em' }, // No letter spacing
      },
      lineHeights: {
        'alignBulletPoint': { value: '1.45rem'}
      },
      spacing: {
        '7.5': { value: '1.875rem' }, // 30px
      }
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
      container: {
        base: {
          maxW: "100rem"
        }
      },
      button: {
        variants: {
          visual: {
            solid: { bg: "brand.500", color: "text.primary", _hover: { boxShadow: "5px 2px 5px 0px rgba(56, 69, 60, 0.35) inset", bg: "brand.500" }, _active: { bg: "brand.600", color: "white" } },
            outline: { borderWidth: "2px", borderColor: "brand.500", color: "white", bg: "transparent", _hover: { bg: "brand.100", color: "text.primary", boxShadow: "5px 2px 5px 0px rgba(56, 69, 60, 0.35) inset" }, _active: { bg: "brand.500", color: "text.primary" } },
            circle: {
              pt: "6",
              pb: "6",
              pl: "4",
              pr: "4",
              borderRadius: "full",
              fontSize: "base",
              boxShadow: "5px 5px 10px 0px rgba(16, 55, 92, 0.25)",
              _hover: { bg: "gray.50" },
              _active: { bg: "gray.100" },
              _disabled: { bg: "gray.200", color: "gray.500", cursor: "not-allowed" },
              color: "text.primary",
              bg: "white",
              w: "3rem",
              h: "3rem",
              minW: "fit-content",
              minH: "fit-content",
            },
            plain: {
              pt: "1",
              pb: "1",
              pl: "1",
              pr: "1",
              fontSize: "base",
              _hover: { color: "gray.50" },
              _active: { color: "gray.100" },
              _disabled: { color: "gray.500", cursor: "not-allowed" },
              color: "text.primary",
              w: "3rem",
              h: "3rem",
              minW: "fit-content",
              minH: "fit-content",
              bg: "transparent",
              border: "none",
              boxShadow: "none",
              borderRadius: "0",
            }
          },
          size: {
            sm: { padding: "2.5", borderRadius: "buttonSm", fontSize: "base", fontWeight: "bold", minW: "minWSm", minH: "10", lineHeight: "moderate", letterSpacing: "default" },
            md: { padding: "4", fontSize: "base", borderRadius: "buttonLg", minW: "minWLg", minH: "minHlg", gap: "2.5", fontWeight: "bold", lineHeight: "moderate", letterSpacing: "empty" },
            lg: { padding: "pLg", fontSize: "base", borderRadius: "buttonLg", minW: "minWLg", minH: "minHlg", gap: "2.5", fontWeight: "bold", lineHeight: "moderate", letterSpacing: "empty" },
          }
        }
      },
      heading: {
        variants: {
          variant: {
            default: { color: "text.primary", fontWeight: "black", textAlign: "center" },
            mainTitle: { color: "white", fontWeight: "black" }
          },
          size: {
            md: { fontSize: "subGiant", lineHeight: "moderate" },
            lg: { fontSize: "giant", lineHeight: "moderate" }
          }
        }
      },
      text: {
        variants: {
          variant: {
            subTitle: { color: "text.primary", fontSize: "xl", fontWeight: "normal", textAlign: "center" },
          }
        }
      }
    }
  },
  globalCss: {
    body: {
      bg: 'white',
      color: '{colors.text.primary}',
      fontSize: '{fontSizes.base}',
      fontFamily: '{fonts.body}',
      letterSpacing: '{letterSpacings.default}',
      lineHeight: 'moderate'
    },
  }
})

// Note:
// Updating theme, may require to execute `npx @chakra-ui/cli typegen ./src/theme.ts`
// to generate the types for the new theme configuration.

const system = createSystem(defaultConfig, config)


export default system
