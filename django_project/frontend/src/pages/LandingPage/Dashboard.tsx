import React, { useState, useEffect } from 'react';
import {
  Box,
  Button,
  Container,
  Flex,
  Grid,
  GridItem,
  Heading,
  HStack,
  Image,
  Link,
  SimpleGrid,
  Spacer,
  Stack,
  Text,
  VStack,
  Separator as Divider,
  Icon,
  IconButton
} from '@chakra-ui/react';

interface NavItem {
  label: string;
  href: string;
}

interface PartnerLogo {
  name: string;
  logo: string;
  website: string;
}

interface ServiceCard {
  title: string;
  items: string[];
}

const handleSmoothScroll = (href: string) => {
  const targetId = href.substring(1); // Remove the '#'
  const targetElement = document.getElementById(targetId);
  
  if (targetElement) {
    // Different offset for mobile vs desktop
    const isMobile = window.innerWidth < 768;
    const offsetTop = targetElement.offsetTop - (isMobile ? 20 : 80); // No navbar on mobile
    window.scrollTo({
      top: offsetTop,
      behavior: 'smooth'
    });
  }
};

const GlobalAccessPlatform: React.FC = () => {
  const [currentSlide, setCurrentSlide] = useState(0);
  const [isAutoPlaying, setIsAutoPlaying] = useState(true);
  const [activeSection, setActiveSection] = useState('');

  const navItems: NavItem[] = [
    { label: 'Bridging The Gap', href: '#bridging' },
    { label: 'Global Impact', href: '#impact' },
    { label: 'Agro-Met Intelligence Hub', href: '#hub' },
    { label: 'Our Partners', href: '#partners' },
    { label: 'About Us', href: '#about' }
  ];

  const partners: PartnerLogo[] = [
    { name: 'Microsoft', logo: '/api/placeholder/150/60', website: 'https://microsoft.com' },
    { name: 'Google', logo: '/api/placeholder/150/60', website: 'https://google.com' },
    { name: 'Amazon', logo: '/api/placeholder/150/60', website: 'https://amazon.com' },
    { name: 'IBM', logo: '/api/placeholder/150/60', website: 'https://ibm.com' },
    { name: 'Oracle', logo: '/api/placeholder/150/60', website: 'https://oracle.com' },
    { name: 'Salesforce', logo: '/api/placeholder/150/60', website: 'https://salesforce.com' }
  ];

  const aboutPartners: PartnerLogo[] = [
    { name: 'Tomorrow.io', logo: '/api/placeholder/150/60', website: 'https://salesforce.com' },
    { name: 'Kartozoa', logo: '/api/placeholder/150/60', website: 'https://salesforce.com' },
    { name: 'Gates Foundation', logo: '/api/placeholder/150/60', website: 'https://salesforce.com' }
  ];

  const services: ServiceCard[] = [
    {
      title: 'Weather & Climate Data',
      items: [
        'Real-time Weather Information',
        'Long-term Forecasts',
        'Climate Data Analysis',
        'Historical Weather Data',
        'Seasonal Weather-based Advisories'
      ]
    },
    {
      title: 'Validation & Localization Suite',
      items: [
        'Data Quality Assessment',
        'Ground-based Observations',
        'Satellite Data Integration',
        'Local Advisory Validation Tools'
      ]
    },
    {
      title: 'Agro-Met Insights',
      items: [
        'Crop & Livestock Best-value CADS Portal',
        'Risk Management Tools',
        'Localized Advisory Services (LOCAS)',
        'Early Warning Systems',
        'Post Harvest Advisory Support'
      ]
    }
  ];

  const bgColor = 'white';
  const textColor = 'text.primary';
  const mutedColor = 'text.secondary';

  // Auto-play carousel
  useEffect(() => {
    if (!isAutoPlaying) return;
    
    const timer = setInterval(() => {
      setCurrentSlide((prev) => (prev + 1) % Math.ceil(partners.length / 3));
    }, 3000); // Change slide every 3 seconds

    return () => clearInterval(timer);
  }, [isAutoPlaying, partners.length]);

  // Scroll spy for active section highlighting
  useEffect(() => {
    const handleScroll = () => {
      const sections = ['bridging', 'impact', 'hub', 'partners', 'about'];
      const scrollPosition = window.scrollY + 100; // Offset for better UX

      for (const section of sections) {
        const element = document.getElementById(section);
        if (element) {
          let { offsetTop, offsetHeight } = element;
          if (section === 'partners') {
            offsetHeight = offsetHeight - 100;
          } else if (section === 'about') {
            offsetTop = offsetTop - 50;
          }
          if (scrollPosition >= offsetTop && scrollPosition < offsetTop + offsetHeight) {
            setActiveSection(`#${section}`);
            break;
          }
        }
      }

      // Clear active section if we're at the top
      if (window.scrollY < 100) {
        setActiveSection('');
      }
    };

    window.addEventListener('scroll', handleScroll);
    handleScroll(); // Check initial position

    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  const nextSlide = () => {
    setCurrentSlide((prev) => (prev + 1) % Math.ceil(partners.length / 3));
    setIsAutoPlaying(false); // Stop auto-play when user manually navigates
  };

  const prevSlide = () => {
    setCurrentSlide((prev) => (prev - 1 + Math.ceil(partners.length / 3)) % Math.ceil(partners.length / 3));
    setIsAutoPlaying(false); // Stop auto-play when user manually navigates
  };

  const goToSlide = (index: number) => {
    setCurrentSlide(index);
    setIsAutoPlaying(false);
  };

  const getVisiblePartners = () => {
    const partnersPerSlide = 3;
    const start = currentSlide * partnersPerSlide;
    return partners.slice(start, start + partnersPerSlide);
  };

  return (
    <Box bg={bgColor} color={textColor} minH="100vh" w="full">
      {/* Navigation */}
      <Box as="nav" bg="white" boxShadow="sm" position="sticky" top={0} zIndex={1000} w="full">
        <Container maxW="7xl" px={{ base: 4, md: 6 }} w="full">
          <Flex h={16} alignItems="center">
            <HStack gap={2} alignItems="center">
              <Box w={8} h={8} bg="green.100" borderRadius="md" />
              <Text fontWeight="bold" fontSize="lg">Global Access Platform</Text>
            </HStack>
            <Spacer />
            <HStack gap={8} display={{ base: 'none', md: 'flex' }}>
              {navItems.map((item) => (
                <Link
                  key={item.label}
                  href={item.href}
                  onClick={(e) => {
                    e.preventDefault();
                    handleSmoothScroll(item.href);
                  }}
                  fontSize="sm"
                  fontWeight={activeSection === item.href ? "bold" : "medium"}
                  color={activeSection === item.href ? "brand.500" : "text.secondary"}
                  _hover={{ color: 'brand.500' }}
                  position="relative"
                  transition="all 0.2s ease"
                  _after={activeSection === item.href ? {
                    content: '""',
                    position: 'absolute',
                    bottom: '-4px',
                    left: '0',
                    right: '0',
                    height: '2px',
                    bg: 'brand.500',
                    borderRadius: 'full',
                  } : {}}
                >
                  {item.label}
                </Link>
              ))}
            </HStack>
            <Button colorScheme="brand" size="sm" ml={4}>
              Join Us
            </Button>
          </Flex>
        </Container>
      </Box>

      {/* Hero Section */}
      <Box
        bgImage="linear-gradient(rgba(0,0,0,0.4), rgba(0,0,0,0.4)), url('/static/images/home.webp')"
        bgSize="cover"
        // bgPosition="center"
        bgAttachment="fixed"
        h="100vh"
        display="flex"
        alignItems="center"
        color="white"
      >
        <Container maxW="7xl" px={{ base: 4, md: 6 }} w="full">
          <VStack gap={6} alignItems="flex-start" maxW={{ base: "full", xl: "xl" }} w="full">
            <Heading as="h1" size={{ base: "2xl", md: "4xl" }} fontWeight="bold" lineHeight="shorter">
              Global Access Platform
            </Heading>
            <Text fontSize={{ base: "xl", md: "2xl" }} fontWeight="medium" color="brand.300">
              For Agro-Met Intelligence
            </Text>
            <Text fontSize={{ base: "md", md: "lg" }} opacity={0.9} maxW="md">
              Unlocking Weather Resilience for Sustainable Farming
            </Text>
            <Button colorScheme="brand" size={{ base: "md", md: "lg" }} px={{ base: 6, md: 8 }} py={{ base: 4, md: 6 }}>
              Get Started
            </Button>
          </VStack>
        </Container>
      </Box>

      {/* Bridging The Gap Section */}
      <Box id="bridging" py={20} bg="gray.50">
        <Container maxW="7xl" px={{ base: 4, md: 6 }} w="full">
          <VStack gap={12}>
            <Heading as="h2" size={{ base: "xl", md: "2xl" }} textAlign="center" color="text.primary">
              Bridging The Gap
            </Heading>
            
            <Box w="full" maxW="4xl">
              <Image
                src="/api/placeholder/800/400"
                alt="Bridging the gap infographic"
                w="full"
                borderRadius="lg"
                boxShadow="lg"
              />
            </Box>

            <VStack gap={6} maxW="3xl" textAlign="center">
              <Heading as="h3" size={{ base: "md", md: "lg" }} color="text.primary">
                What is Agro-Met Intelligence?
              </Heading>
              <Text fontSize={{ base: "md", md: "lg" }} color="text.secondary" lineHeight="tall">
                Agro-Met Intelligence combines meteorological data with agricultural insights to provide farmers with
                complete, localized, and decision-support tools to optimize agricultural practices, enhance crop productivity, and
                manage risks. It ensures collecting and accessing meteorological information alongside agronomic data to provide
                actionable intelligence for sustainable farming decisions.
              </Text>
            </VStack>
          </VStack>
        </Container>
      </Box>

      {/* Global Impact Section */}
      <Box id="impact" py={20}>
        <Container maxW="7xl" px={{ base: 4, md: 6 }} w="full">
          <VStack gap={12}>
            <VStack gap={4} textAlign="center">
              <Heading as="h2" size={{ base: "xl", md: "2xl" }} color="text.primary">
                Global Impact
              </Heading>
              <Text fontSize={{ base: "md", md: "lg" }} color="text.secondary">
                Our reach via partner like Mali, Weather Content Partner Across Africa
              </Text>
            </VStack>

            <Grid templateColumns={{ base: '1fr', lg: '1fr 1fr' }} gap={12} alignItems="center">
              <GridItem>
                <Image
                  src="/api/placeholder/500/400"
                  alt="Africa map showing global impact"
                  w="full"
                  borderRadius="lg"
                />
              </GridItem>
              <GridItem>
                <VStack gap={8} align="stretch">
                  <Box bg="white" p={6} borderRadius="lg" boxShadow="md" border="1px solid" borderColor="gray.200">
                    <VStack gap={4} align="start">
                      <Heading as="h3" size="md" color="brand.600">
                        The Problem
                      </Heading>
                      <Text color="text.secondary">
                        Smallholder farmers face increasing weather uncertainty with limited
                        access to reliable, localized weather information and value they need to make
                        informed decisions about when to plant, irrigate, and harvest their crops.
                      </Text>
                    </VStack>
                  </Box>
                  
                  <Box bg="white" p={6} borderRadius="lg" boxShadow="md" border="1px solid" borderColor="gray.200">
                    <VStack gap={4} align="start">
                      <Heading as="h3" size="md" color="brand.500">
                        Our Solution
                      </Heading>
                      <Text color="text.secondary">
                        Localized weather and agricultural advisories delivered
                        through accessible channels. We process weather data through machine
                        learning models to provide timely, relevant insights and alert the appropriate
                        agricultural weather programs responsible to build the organizational
                        resilience the climate adaptation needs.
                      </Text>
                    </VStack>
                  </Box>
                </VStack>
              </GridItem>
            </Grid>
          </VStack>
        </Container>
      </Box>

      {/* Agro-Met Intelligence Hub */}
      <Box
        id="hub"
        py={20}
        bgImage="linear-gradient(rgba(0,0,0,0.7), rgba(0,0,0,0.7)), url('/api/placeholder/1920/600')"
        bgSize="cover"
        // bgPosition="center"
        color="white"
      >
        <Container maxW="7xl" px={{ base: 4, md: 6 }} w="full">
          <VStack gap={12}>
            <VStack gap={4} textAlign="center">
              <Heading as="h2" size="2xl">
                Agro-Met Intelligence Hub
              </Heading>
              <Text fontSize="lg" opacity={0.9} maxW="2xl">
                Trusted weather insights for agriculture via Real-time & cloud-based intelligence platform (RTCIP) built with advanced
                weather APIs
              </Text>
            </VStack>

            <SimpleGrid columns={{ base: 1, md: 3 }} gap={{ base: 6, md: 8 }} w="full">
              {services.map((service, index) => (
                <Box key={index} bg="whiteAlpha.100" backdropFilter="blur(10px)" border="1px solid" borderColor="whiteAlpha.200" p={6} borderRadius="lg">
                  <VStack gap={4} align="start">
                    <Heading as="h3" size="md" color="white">
                      {service.title}
                    </Heading>
                    <VStack gap={2} align="start" w="full">
                      {service.items.map((item, itemIndex) => (
                        <HStack key={itemIndex} gap={2} align="start">
                          <Box w={2} h={2} bg="brand.400" borderRadius="full" mt={2} flexShrink={0} />
                          <Text fontSize="sm" opacity={0.9}>
                            {item}
                          </Text>
                        </HStack>
                      ))}
                    </VStack>
                  </VStack>
                </Box>
              ))}
            </SimpleGrid>
          </VStack>
        </Container>
      </Box>

      {/* Partners Section */}
      <Box id={"partners"} py={20} bg="white">
        <Container maxW="7xl" px={{ base: 4, md: 6 }} w="full">
          <VStack gap={12}>
            <VStack gap={4} textAlign="center">
              <Heading as="h2" size={{ base: "xl", md: "2xl" }} color="text.primary">
                Meet Our Partners
              </Heading>
              <Text fontSize={{ base: "md", md: "lg" }} color="text.secondary">
                Collaborating with world-leading organizations to drive innovation in agro-met intelligence
              </Text>
            </VStack>

            <Box position="relative" w="full" maxW="4xl">
              {/* Carousel Container */}
              <Box overflow="hidden" borderRadius="lg">
                <Flex
                  transform={`translateX(-${currentSlide * 100}%)`}
                  transition="transform 0.5s ease-in-out"
                  w={`${Math.ceil(partners.length / 3) * 100}%`}
                >
                  {Array.from({ length: Math.ceil(partners.length / 3) }).map((_, slideIndex) => (
                    <Flex key={slideIndex} w="100%" justify="center" gap={8} py={8}>
                      {partners.slice(slideIndex * 3, (slideIndex + 1) * 3).map((partner, index) => (
                        <Box
                          key={index}
                          as="a"
                          // href={partner.website}
                          // target="_blank"
                          rel="noopener noreferrer"
                          opacity={0.7}
                          _hover={{ 
                            opacity: 1, 
                            transform: 'scale(1.05)',
                            cursor: 'pointer'
                          }}
                          transition="all 0.3s ease"
                          p={4}
                          bg="white"
                          borderRadius="lg"
                          boxShadow="md"
                          border="1px solid"
                          borderColor="gray.200"
                          minW="150px"
                          display="flex"
                          alignItems="center"
                          justifyContent="center"
                        >
                          <Image 
                            src={partner.logo} 
                            alt={partner.name} 
                            h={12} 
                            maxW="120px"
                            objectFit="contain"
                          />
                        </Box>
                      ))}
                    </Flex>
                  ))}
                </Flex>
              </Box>

              {/* Navigation Arrows */}
              <IconButton
                aria-label="Previous partners"
                position="absolute"
                left={-12}
                top="50%"
                transform="translateY(-50%)"
                onClick={prevSlide}
                bg="white"
                border="1px solid"
                borderColor="gray.300"
                borderRadius="full"
                size="lg"
                _hover={{ bg: 'gray.50' }}
                boxShadow="md"
              >
                <Box as="span" fontSize="20px">‹</Box>
              </IconButton>

              <IconButton
                aria-label="Next partners"
                position="absolute"
                right={-12}
                top="50%"
                transform="translateY(-50%)"
                onClick={nextSlide}
                bg="white"
                border="1px solid"
                borderColor="gray.300"
                borderRadius="full"
                size="lg"
                _hover={{ bg: 'gray.50' }}
                boxShadow="md"
              >
                <Box as="span" fontSize="20px">›</Box>
              </IconButton>

              {/* Dots Indicator */}
              <HStack gap={2} justify="center" mt={6}>
                {Array.from({ length: Math.ceil(partners.length / 3) }).map((_, index) => (
                  <Button
                    key={index}
                    onClick={() => goToSlide(index)}
                    w={3}
                    h={3}
                    minW={3}
                    borderRadius="full"
                    bg={currentSlide === index ? 'brand.500' : 'gray.300'}
                    _hover={{ bg: currentSlide === index ? 'brand.600' : 'gray.400' }}
                    transition="all 0.2s"
                    p={0}
                  />
                ))}
              </HStack>
            </Box>

            <Button colorScheme="brand" size="lg">
              Explore Partnerships
            </Button>
          </VStack>
        </Container>
      </Box>

      {/* About Us Section */}
      <Box id="about" py={20} bg="gray.50">
        <Container maxW="7xl" px={{ base: 4, md: 6 }} w="full">
          <VStack gap={12}>
            <VStack gap={4} textAlign="center">
              <Heading as="h2" size={{ base: "xl", md: "2xl" }} color="text.primary">
                About Us
              </Heading>
              <Text fontSize={{ base: "md", md: "lg" }} color="text.secondary" maxW="2xl">
                The Global Access Platform is built and managed by Tomorrow.io and Kartozoa
                with funding generously provided by The Gates Foundation.
              </Text>
            </VStack>

            <SimpleGrid columns={{ base: 1, md: 3 }} gap={{ base: 6, md: 8 }} w="full">
              {aboutPartners.map((partner, index) => (
                <Box key={index} bg="white" p={6} borderRadius="lg" boxShadow="md" border="1px solid" borderColor="gray.200">
                  <Box textAlign="center">
                    <Image src={partner.logo} alt={partner.name} h={16} mx="auto" mb={4} />
                  </Box>
                </Box>
              ))}
            </SimpleGrid>

            <Button colorScheme="brand" size="lg">
              Learn More About Our Mission
            </Button>
          </VStack>
        </Container>
      </Box>

      {/* Footer */}
      <Box bg="gray.800" color="white" py={12}>
        <Container maxW="7xl" px={{ base: 4, md: 6 }} w="full">
          <Grid templateColumns={{ base: '1fr', md: 'repeat(4, 1fr)' }} gap={8}>
            <GridItem>
              <VStack gap={4} align="start">
                <HStack gap={2}>
                  <Box w={8} h={8} bg="brand.500" borderRadius="md" />
                  <Text fontWeight="bold">Global Access Platform (GAP)</Text>
                </HStack>
                <Text fontSize="sm" color="text.muted">
                  Agro-met intelligence platform built for smallholder farmers worldwide.
                </Text>
              </VStack>
            </GridItem>

            <GridItem>
              <VStack gap={4} align="start">
                <Heading as="h4" size="sm">Resources</Heading>
                <VStack gap={2} align="start">
                  <Link fontSize="sm" color="text.muted" _hover={{ color: 'white' }}>Documentation</Link>
                  <Link fontSize="sm" color="text.muted" _hover={{ color: 'white' }}>Global Access Standards</Link>
                  <Link fontSize="sm" color="text.muted" _hover={{ color: 'white' }}>Weather Prediction Package</Link>
                </VStack>
              </VStack>
            </GridItem>

            <GridItem>
              <VStack gap={4} align="start">
                <Heading as="h4" size="sm">Contact Us</Heading>
                <VStack gap={2} align="start">
                  <Text fontSize="sm" color="text.muted">support@globalaccess.org</Text>
                  <Text fontSize="sm" color="text.muted">+1 (555) 123-4567</Text>
                </VStack>
              </VStack>
            </GridItem>

            <GridItem>
              <VStack gap={4} align="start">
                <Button colorScheme="brand" size="sm" w="full">
                  Get Started Today
                </Button>
              </VStack>
            </GridItem>
          </Grid>

          <Divider my={8} borderColor="gray.600" />
          
          <Flex justify="space-between" align="center" flexDirection={{ base: 'column', md: 'row' }} gap={4}>
            <Text fontSize="sm" color="text.muted">
              © 2024 Global Access Platform. All rights reserved.
            </Text>
            <HStack gap={4}>
              <Link fontSize="sm" color="text.muted" _hover={{ color: 'white' }}>Privacy Policy</Link>
              <Link fontSize="sm" color="text.muted" _hover={{ color: 'white' }}>Terms of Service</Link>
            </HStack>
          </Flex>
        </Container>
      </Box>
    </Box>
  );
};

export default GlobalAccessPlatform;