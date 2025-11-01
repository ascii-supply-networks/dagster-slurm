import React from 'react';
import type {Props} from '@theme/Root';
import {useLocation} from '@docusaurus/router';
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';

const SCARF_PIXEL_BASE =
  'https://static.scarf.sh/a.png?x-pxid=02e4ad6e-79c4-4ba8-b751-4833af626868';

function ScarfPixel(): JSX.Element | null {
  const location = useLocation();

  if (!ExecutionEnvironment.canUseDOM) {
    return null;
  }

  const pageParam = `&page=${encodeURIComponent(location.pathname || 'unknown')}`;
  const pixelSrc = `${SCARF_PIXEL_BASE}${pageParam}`;

  return (
    <img
      aria-hidden="true"
      referrerPolicy="no-referrer"
      src={pixelSrc}
      alt=""
      style={{position: 'absolute', width: 1, height: 1, opacity: 0}}
    />
  );
}

export default function Root({children}: Props): JSX.Element {
  return (
    <>
      {children}
      <ScarfPixel />
    </>
  );
}
