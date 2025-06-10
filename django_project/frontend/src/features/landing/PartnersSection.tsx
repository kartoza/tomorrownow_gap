import React from "react";
import {
    Box,
    Button,
    Container,
    Heading,
    Text,
    VStack,
    Image,
    IconButton
} from "@chakra-ui/react";
import { Swiper, SwiperSlide } from "swiper/react";
import { Navigation, Pagination, Autoplay } from 'swiper/modules';
import "swiper/css";
import "swiper/css/navigation";
import "swiper/css/pagination";
import { ArrowNavResponsiveIcon } from "./ArrowNavResponsiveIcon";
import { openInNewTab } from "@/utils/url";
import { partners } from "./types";
import { useNavigateWithEvent } from "@/hooks/useNavigateWithEvent";


const PartnersSection: React.FC = () => {
    const navigate = useNavigateWithEvent();
    return (
        <Box id={"partners"} py={20} bg="white">
            <Container px={{ base: 4, md: 6 }} w="full">
                <VStack gap={12}>
                <VStack gap={4} textAlign="center">
                    <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
                        Meet Our Partners
                    </Heading>
                    <Text variant={"subTitle"}>
                        Collaborating with world-leading organizations to drive innovation in agro-met intelligence.
                    </Text>
                </VStack>
                
                {/* Swiper Carousel */}
                <Box position="relative" w="full">
                    <Swiper
                    modules={[Navigation, Pagination, Autoplay ]}
                    spaceBetween={100}
                    slidesPerView={1}
                    navigation={{
                        nextEl: '.swiper-button-next-custom',
                        prevEl: '.swiper-button-prev-custom',
                    }}
                    pagination={{
                        clickable: true
                    }}
                    autoplay={{
                        delay: 3000,
                        disableOnInteraction: false,                  
                    }}
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
                        slidesPerView: 2,
                        spaceBetween: 100,
                        },
                        1280: {
                        slidesPerView: 3,
                        spaceBetween: 100,
                        },
                        1536: {
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
                            onClick={() => openInNewTab(partner.website, partner.name)}
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
    
                <Button visual="solid" size="md" onClick={() => navigate('/signup', 'partners_section_become_partner')}>
                    Become a Partner
                </Button>
                </VStack>
            </Container>
        </Box>
    );
};

export default PartnersSection;