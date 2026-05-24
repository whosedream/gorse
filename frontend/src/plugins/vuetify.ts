import 'vuetify/styles'
import { createVuetify } from 'vuetify'

export const vuetify = createVuetify({
  theme: {
    defaultTheme: 'rerankDark',
    themes: {
      rerankDark: {
        dark: true,
        colors: {
          background: '#08111f',
          surface: '#111b2d',
          primary: '#66e3ff',
          secondary: '#9b7cff',
          accent: '#19f5a8',
          error: '#ff5c7a',
          info: '#5cc8ff',
          success: '#27f5a6',
          warning: '#ffc857',
        },
      },
    },
  },
  defaults: {
    VCard: {
      rounded: 'xl',
    },
    VChip: {
      density: 'comfortable',
    },
    VBtn: {
      rounded: 'lg',
    },
  },
})
