import React from 'react';
import { useBreakpointValue } from "@chakra-ui/react";
import { FiArrowLeft, FiArrowRight, FiChevronLeft, FiChevronRight } from "react-icons/fi";


export const ArrowNavResponsiveIcon = (pos: "left" | "right") => {
  if (pos === "left") {
    const LeftIcon = useBreakpointValue({ base: FiChevronLeft, md: FiArrowLeft });
    return <LeftIcon/>;
  }

  const Icon = useBreakpointValue({ base: FiChevronRight, md: FiArrowRight });
  return <Icon/>;
};