/**
 * Merge class names. For Tailwind, prefer installing clsx + tailwind-merge and:
 *   export function cn(...inputs: ClassValue[]) { return twMerge(clsx(inputs)); }
 */
export function cn(...inputs: (string | undefined | null | false)[]): string {
  return inputs.filter(Boolean).join(" ");
}
