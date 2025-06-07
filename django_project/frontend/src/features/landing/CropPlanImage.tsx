import React from 'react';
import { useBreakpointValue } from "@chakra-ui/react";
import { Image } from "@chakra-ui/react";


export const CropPlanImage = () => {
  const isMobile = useBreakpointValue({ base: true, md: false });
  return (
    <Image
      src={isMobile ? "/static/images/crop_plan_mobile.webp" : "/static/images/crop_plan.webp"}
      alt="Bridging the gap infographic"
      w="full"
    />
  );
}