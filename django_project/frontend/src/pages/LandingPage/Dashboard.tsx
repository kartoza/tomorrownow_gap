import React, { useState, useEffect } from 'react';
import { Swiper, SwiperSlide } from 'swiper/react';
import { Navigation, Pagination, Autoplay } from 'swiper/modules';
import 'swiper/css';
import 'swiper/css/navigation';
import 'swiper/css/pagination';
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
  IconButton,
  useBreakpointValue
} from '@chakra-ui/react';
import { FiArrowLeft, FiArrowRight, FiChevronLeft, FiChevronRight } from "react-icons/fi";

interface NavItem {
  label: string;
  href: string;
}

interface PartnerLogo {
  name: string;
  logo: string;
  website: string;
  mt?: string; // Optional margin-top for spacing
}

interface ServiceCard {
  title: string;
  items: string[];
  icon: string;
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

const openInNewTab = (url: string) => {
  window.open(url, '_blank', 'noopener,noreferrer');
};

const ArrowNavResponsiveIcon = (pos: "left" | "right") => {
  if (pos === "left") {
    const LeftIcon = useBreakpointValue({ base: FiChevronLeft, md: FiArrowLeft });
    return <LeftIcon/>;
  }

  const Icon = useBreakpointValue({ base: FiChevronRight, md: FiArrowRight });
  return <Icon/>;
};

const APIDocsURL = '/api/v1/docs/';

const GlobalAccessPlatform: React.FC = () => {
  const [activeSection, setActiveSection] = useState('');

  const navItems: NavItem[] = [
    { label: 'Products', href: '#hub' },
    { label: 'Data Access', href: APIDocsURL },
    { label: 'Our Partners', href: '#partners' },
    { label: 'About Us', href: '#about' }
  ];

  const partners: PartnerLogo[] = [
    { name: 'CGIAR', logo: '/static/images/cgiar.webp', website: 'https://www.cgiar.org' },
    { name: 'ONE ACRE FUND', logo: '/static/images/oneacrefund.webp', website: 'https://oneacrefund.org' },
    { name: 'Regen Organics', logo: '/static/images/regen_organics.webp', website: 'https://www.regenorganics.co', mt: '1.5rem' },
    { name: 'Rhiza Research', logo: '/static/images/rhiza_v5.webp', website: 'https://rhizaresearch.org' },
    { name: 'Salient', logo: '/static/images/salient.webp', website: 'https://www.salientpredictions.com', mt: '0.2rem' },
    { name: 'Tahmo', logo: '/static/images/tahmo.webp', website: 'https://tahmo.org', mt: '-2rem' },
    { name: 'Tomorrow.io', logo: '/static/images/tomorrow.io.webp', website: 'https://www.tomorrow.io', mt: '1rem' },
  ];

  const aboutPartners: PartnerLogo[] = [
    { name: 'TomorrowNow', logo: '/static/images/tomorrownow.webp', website: 'https://tomorrownow.org' },
    { name: 'Kartoza', logo: '/static/images/kartoza.webp', website: 'https://kartoza.com' },
    { name: 'Gates Foundation', logo: '/static/images/gates_foundation.webp', website: 'https://www.gatesfoundation.org' }
  ];

  const services: ServiceCard[] = [
    {
      icon: '/static/images/weather.webp',
      title: 'Weather & Climate Data',
      items: [
        'Historical Climate Reanalysis',
        'Long-term Normals',
        'Medium-Range Forecasts',
        'Seasonal Forecasts',
        'Quality-Controlled Ground Observations',
      ]
    },
    {
      icon: '/static/images/validation.webp',
      title: 'Validation & Localization Suite',
      items: [
        'Forecast Validation Dashboard',
        'Forecast Modeling Benchmarking',
        'Ground Statino QA/QC Tools',
        'Digital Advisory Evaluation Tools'
      ]
    },
    {
      icon: '/static/images/insight.webp',
      title: 'AgroMet Insights',
      items: [
        'Planting Window Advisories',
        'Pest & Disease Risk Alerts',
        'Crop Variety Assessment',
        'Digital Crop Advisory Service',
        'Digital Weather Advisories Service'
      ]
    }
  ];

  const bgColor = 'white';
  const textColor = 'text.primary';

  // Scroll spy for active section highlighting
  useEffect(() => {
    const handleScroll = () => {
      const sections = ['hub', 'partners', 'about'];
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


  return (
    <Box bg={bgColor} color={textColor} minH="100vh" w="full">
      {/* Navigation */}
      <Box as="nav" bg="white" boxShadow="sm" position="sticky" top={0} zIndex={1000} w="full">
        <Container px={{ base: 4, md: 6 }} w="full">
          <Flex h={16} alignItems="center">
            <HStack gap={2} alignItems="center">
              <Box w={'23px'} h={'25px'} bgSize={"contain"} bgPos="center" bgImage="url('/static/images/gap.png')" />
              <Text fontWeight="bold">Global Access Platform</Text>
            </HStack>
            <Spacer />
            <HStack gap={8} display={{ base: 'none', md: 'flex' }}>
              {navItems.map((item) => (
                <Link
                  key={item.label}
                  href={item.href}
                  onClick={(e) => {
                    e.preventDefault();
                    if (item.href.startsWith('#')) {
                      handleSmoothScroll(item.href);
                    } else if (item.href !== '') {
                      openInNewTab(item.href);
                    }
                  }}
                  fontWeight={activeSection === item.href && item.href != '' ? "bold" : "medium"}
                  color={activeSection === item.href && item.href != '' ? "brand.500" : "text.primary"}
                  _hover={{ color: 'brand.500' }}
                  position="relative"
                  transition="all 0.2s ease"
                  _after={activeSection === item.href && item.href != '' ? {
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
            <Button visual="solid" size="sm" ml={4}>
              Log In
            </Button>
          </Flex>
        </Container>
      </Box>

      {/* Hero Section */}
      <Box
        h="100vh"
        display="flex"
        alignItems="center"
        color="white"
        position={'relative'}
        bgImage="url('/static/images/home.webp')"
        bgSize="cover"
        bgPos="center"
        bgAttachment="fixed"
        w="full"
      >
        <Box h='100vh' w="full" display="flex" alignItems="center" position="relative" backdropFilter={'blur(2px)'} bgColor="rgba(0, 213, 142, 0.05)">
          <Container px={{ base: 9, md: 16 }} w="full">
            <VStack gap={6} alignItems={{ base: "center", md: "flex-start" }} w="full">
              <Heading as="h1" size={{ base: "md", md: "lg" }} variant={"mainTitle"}>
                Global Access Platform
              </Heading>
              <Text fontSize={{ base: "3xl", md: "subGiant" }} fontWeight="extrabold" lineHeight="moderate">
                For Agro-Met Intelligence
              </Text>
              <Text fontSize="2xl" fontWeight="extrabold" mt={5}>
                Unlocking Weather Resilience for Smallholder Farming
              </Text>
              <Button visual="solid" size="md">
                Join Us
              </Button>
            </VStack>
          </Container>
        </Box>
      </Box>

      {/* Bridging The Gap Section */}
      <Box id="bridging" py={20}>
        <Container px={{ base: 4, md: 6 }} w="full">
          <VStack gap={12}>
            <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
              Bridging The Gap
            </Heading>
            <Text variant={"subTitle"}>
              Agro-Met intelligence is the integration of weather, climate, and agronomic data with advanced analytics to provide actionable insights that optimise farming, boost productivity, and manage agricultural risks.
            </Text>
            <Box w="full">
              <Image
                src="/static/images/crop_plan.webp"
                alt="Bridging the gap infographic"
                w="full"
              />
            </Box>
          </VStack>
        </Container>
      </Box>

      {/* Agro-Met Intelligence Hub */}
      <Box
        id="hub"
        py={20}
        bgImage="url('/static/images/bg_hub.webp')"
        bgSize="cover"
        bgPos="top"
        color="white"
      >
        <Container px={{ base: 4, md: 6 }} w="full">
          <VStack gap={12}>
            <VStack gap={4} textAlign="center">
              <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
                Agro-Met Intelligence Hub
              </Heading>
              <Text variant={"subTitle"}>
                Trusted, localized insights for organizations on the front lines of helping farmers increase productivity and resilience.
              </Text>
            </VStack>

            <SimpleGrid columns={{ base: 1, md: 3 }} gap={{ base: 6, md: 8 }} maxW={{base: "full", md: "70%"}}>
              {services.map((service, index) => (
                <Box key={index} bg="white"  p={7.5} borderRadius="lg" boxShadow="5px 5px 10px 0px rgba(16, 55, 92, 0.25)">
                  <VStack gap={4} align="start">
                    <HStack align="center" w="full" justify={"center"}>
                      <Box w={'55px'} h={'55px'} bgSize={"contain"} bgPos="center" bgImage={`url('${service.icon}')`} />
                    </HStack>
                    <Heading as="h3" fontSize={'xl'} color="text.primary" alignItems={"center"} w={"full"}>
                      {service.title}
                    </Heading>
                    <VStack gap={2} align="start" w="full">
                      {service.items.map((item, itemIndex) => (
                        <HStack key={itemIndex} gap={2} align="start">
                          <Box w={2} h={2} bg="brand.600" borderRadius="full" mt={2} flexShrink={0} />
                          <Text fontSize="sm" color="text.primary" lineHeight={"alignBulletPoint"} textAlign={"start"}>
                            {item}
                          </Text>
                        </HStack>
                      ))}
                    </VStack>
                  </VStack>
                </Box>
              ))}
            </SimpleGrid>

            <VStack gap={4} textAlign="center">
              <Button visual="solid" size="md" onClick={() => openInNewTab(APIDocsURL)}>
                See API Docs
              </Button>
            </VStack>
          </VStack>
        </Container>
      </Box>

      {/* Global Impact Section */}
      <Box id="impact" py={20}>
        <Container px={{ base: 4, md: 6 }} w="full">
          <VStack gap={12}>
            <VStack gap={4} textAlign="center">
              <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
                Global Impact
              </Heading>
              <Text variant={"subTitle"}>
                Our north star vision is 100 Million Weather-Resilient Farmers Across Africa.
              </Text>
            </VStack>

            <Grid templateColumns={{ base: '1fr', lg: '1fr 1fr' }} gap={12} alignItems="center">
              <GridItem alignSelf={"stretch"} display={"flex"}>
                <Image
                  src="/static/images/map_africa.webp"
                  alt="Africa map showing global impact"
                  w="full"
                  borderRadius="lg"
                />
              </GridItem>
              <GridItem>
                <VStack gap={8} align="stretch">
                  <Box w="full">
                    <Heading as="h3" size="md" fontWeight={"extrabold"}>
                      What Drives Our Work?
                    </Heading>
                  </Box>
                  <Box bg="white" p={7.5} borderRadius="2lg" boxShadow="5px 5px 10px 2px rgba(56, 69, 60, 0.25)" >
                    <VStack gap={4} align="start">
                      <Heading as="h3" fontSize={"xl"} color="brand.600">
                        The Problem
                      </Heading>
                      <Text color="text.primary" textAlign={"start"} fontStyle={"italic"}>
                        Limited access to actionable weather data
                      </Text>
                      <Text color="text.primary" textAlign={"start"}>
                        Smallholder farmers face increasing weather uncertainty with limited
                        access to reliable, localized weather information and value they need to make
                        informed decisions about when to plant, irrigate, and harvest their crops.
                      </Text>
                    </VStack>
                  </Box>
                  
                  <Box bg="brand.100" p={7.5} borderRadius="2lg" boxShadow="5px 5px 10px 2px rgba(56, 69, 60, 0.25)">
                    <VStack gap={4} align="start">
                      <Heading as="h3" fontSize={"xl"} color="brand.600">
                        Our Solution
                      </Heading>
                      <Text color="text.primary" textAlign={"start"} fontStyle={"italic"}>
                        Agro-Met intelligence - localized, accessible, affordable
                      </Text>
                      <Text color="text.primary" textAlign={"start"}>
                        GAP bridges the first-mile technology with last-mile impact by making advanced weather products accessible and useful for organizations supporting smallholder farmers through more open data, localized products, and  community partnerships.
                      </Text>
                    </VStack>
                  </Box>
                </VStack>
              </GridItem>
            </Grid>
          </VStack>
        </Container>
      </Box>

      

      {/* Partners Section */}
      <Box id={"partners"} py={20} bg="white">
        <Container px={{ base: 4, md: 6 }} w="full">
          <VStack gap={12}>
            <VStack gap={4} textAlign="center">
              <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
                Meet Our Partners
              </Heading>
              <Text variant={"subTitle"}>
                Collaborating with world-leading organizations to drive innovation in agro-met intelligence
              </Text>
            </VStack>
            
            {/* Swiper Carousel */}
            <Box position="relative" w="full">
              <Swiper
                modules={[Navigation, Pagination, Autoplay]}
                spaceBetween={100}
                slidesPerView={1}
                navigation={{
                  nextEl: '.swiper-button-next-custom',
                  prevEl: '.swiper-button-prev-custom',
                }}
                pagination={{
                  clickable: true
                }}
                // autoplay={{
                //   delay: 3000,
                //   disableOnInteraction: false,                  
                // }}
                breakpoints={{
                  480: {
                    slidesPerView: 1,
                    spaceBetween: 20,
                  },
                  640: {
                    slidesPerView: 2,
                    spaceBetween: 100,
                  },
                  768: {
                    slidesPerView: 2,
                    spaceBetween: 100,
                  },
                  1024: {
                    slidesPerView: 4,
                    spaceBetween: 100,
                  },
                }}
                style={{
                  paddingBottom: '3.5rem', // Space for pagination
                  paddingLeft: '3.125rem',   // Space for navigation
                  paddingRight: '3.125rem',  // Space for navigation
                }}
              >
                {partners.map((partner, index) => (
                  <SwiperSlide key={index} >
                    <Box
                      onClick={() => openInNewTab(partner.website)}
                      opacity={0.8}
                      _hover={{ 
                        opacity: 1, 
                        transform: 'scale(1.05)',
                        cursor: 'pointer'
                      }}
                      transition="all 0.3s ease"
                      p={2}
                      h="140px"
                      maxH="140px"
                      display="flex"
                      alignItems="center"
                      justifyContent="center"
                    >
                      <Image 
                        src={partner.logo} 
                        alt={partner.name} 
                        maxH="120px"
                        maxW="280px"
                        w="auto"
                        h="auto"
                        objectFit="contain"
                        objectPosition="center"
                        mt={partner.mt || '0'} // Use optional margin-top if provided
                      />
                    </Box>
                  </SwiperSlide>
                ))}
              </Swiper>

              {/* Custom Navigation Buttons */}
              <IconButton
                className="swiper-button-prev-custom"
                aria-label="Previous partners"
                position="absolute"
                left="-0.625rem"
                top="40%"
                transform="translate(0.625rem, -40%)"
                zIndex={10}
                visual={{ base: "plain", md: "circle" }}
                 css={{
                  "& > svg": {
                    width:  {base: "1.75rem", md: "1.5rem"},
                    height:  {base: "1.75rem", md: "1.5rem"},
                  }
                }}
              >
                {ArrowNavResponsiveIcon("left")}
              </IconButton>

              <IconButton
                className="swiper-button-next-custom"
                aria-label="Next partners"
                position="absolute"
                right="0"
                top="40%"
                transform="translate(0, -40%)"
                zIndex={10}
                visual={{ base: "plain", md: "circle" }}
                 css={{
                  "& > svg": {
                    width:  {base: "1.75rem", md: "1.5rem"},
                    height: {base: "1.75rem", md: "1.5rem"},
                  }
                }}
              >
                {ArrowNavResponsiveIcon("right")}
              </IconButton>
            </Box>

            <Button visual="solid" size="md" onClick={() => window.location.href = APIDocsURL}>
              Become a Partner
            </Button>
          </VStack>
        </Container>
      </Box>

      {/* About Us Section */}
      <Box
        id="about"
        py={20}
        bgImage="url('/static/images/about_us.webp')"
        bgSize="cover"
        bgPos="top"
      >
        <Container px={{ base: 4, md: 6 }} w="full">
          <VStack gap={12}>
            <VStack gap={4} textAlign="center">
              <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
                About Us
              </Heading>
              <Text variant={"subTitle"}>
                The Global Access Platform is built and managed by <span style={{fontWeight: "bold"}}>Tomorrow.io</span> and <span style={{fontWeight: "bold"}}>Kartoza </span> 
                with funding generously provided by <span style={{fontWeight: "bold"}}>The Gates Foundation</span>.
              </Text>
            </VStack>

            <SimpleGrid columns={{ base: 1, md: 3 }} gap={{ base: 6, md: 8 }} w="full">
              {aboutPartners.map((partner, index) => (
                <Box
                  key={index}
                  textAlign="center"
                  alignSelf={"center"}
                  onClick={() => openInNewTab(partner.website)}
                  _hover={{ 
                    opacity: 1, 
                    transform: 'scale(1.05)',
                    cursor: 'pointer'
                  }}
                  transition="all 0.3s ease"
                >
                  <Image src={partner.logo} alt={partner.name} mx="auto" mb={6} objectFit={"contain"} />
                </Box>
              ))}
            </SimpleGrid>
          </VStack>
        </Container>
      </Box>

      {/* Footer */}
      <Box bg="brand.600" color="white" py={12}>
        <Container px={{ base: 4, md: 6 }} w="full">
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